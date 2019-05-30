#!python3

import sys, os, time, logging, sqlite3
import types
from send_email import summon_devteam
import pdb

this_file_dir = os.path.dirname(os.path.abspath(__file__))
method_local_dir = os.path.join(this_file_dir, 'method_local')
containing_dirname = os.path.basename(os.path.dirname(this_file_dir))

from pace_util import (
    pyhamilton, LayoutManager, ResourceType, Plate96, Tip96,
    HamiltonInterface, ClarioStar, LBPumps, PlateData, Shaker,
    initialize, hepa_on, tip_pick_up, tip_eject, aspirate, dispense, wash_empty_refill,
    tip_pick_up_96, tip_eject_96, aspirate_96, dispense_96,
    resource_list_with_prefix, read_plate, move_plate, add_robot_level_log, add_stderr_logging,
    fileflag, clear_fileflag, run_async, yield_in_chunks, log_banner)

def ensure_meas_table_exists(db_conn):
    '''
    Definitions of the fields in this table:
    lagoon_number - the number of the lagoon, uniquely identifying the experiment, zero-indexed
    filename - absolute path to the file in which this data is housed
    plate_id - ID field given when measurement was requested, should match ID in data file
    timestamp - time at which the measurement was taken
    well - the location in the plate reader plate where this sample was read, e.g. 'B2'
    measurement_delay_time - the time, in minutes, after the sample was pipetted that the
                            measurement was taken. For migration, we consider this to be 0
                            minutes in the absense of pipetting time values
    reading - the raw measured value from the plate reader
    data_type - 'lum' 'abs' or the spectra values for the fluorescence measurement
    '''
    c = db_conn.cursor()
    c.execute('''CREATE TABLE if not exists measurements
                (lagoon_number, filename, plate_id, timestamp, well, measurement_delay_time, reading, data_type)''')
    db_conn.commit()

def db_add_plate_data(plate_data, data_type, plate, vessel_numbers, read_wells):
    db_conn = sqlite3.connect(os.path.join(method_local_dir, containing_dirname + '.db'))
    ensure_meas_table_exists(db_conn)
    c = db_conn.cursor()
    for lagoon_number, read_well in zip(vessel_numbers, read_wells):
        filename = plate_data.path
        plate_id = plate_data.header.plate_ids[0]
        timestamp = plate_data.header.time
        well = plate.position_id(read_well)
        measurement_delay_time = 0.0
        reading = plate_data.value_at(*plate.well_coords(read_well))
        data = (lagoon_number, filename, plate_id, timestamp, well, measurement_delay_time, 
                 reading, data_type)
        c.execute("INSERT INTO measurements VALUES (?,?,?,?,?,?,?,?)", data)
    db_conn.commit()
    db_conn.close()

if __name__ == '__main__':
    local_log_dir = os.path.join(method_local_dir, 'log')
    if not os.path.exists(local_log_dir):
        os.mkdir(local_log_dir)
    main_logfile = os.path.join(local_log_dir, 'main.log')
    logging.basicConfig(filename=main_logfile, level=logging.DEBUG, format='[%(asctime)s] %(name)s %(levelname)s %(message)s')
    add_robot_level_log()
    add_stderr_logging()
    for banner_line in log_banner('Begin execution of ' + __file__):
        logging.info(banner_line)

    num_lagoons = 48
    lagoons = range(num_lagoons)
    num_reader_plates = 5 * 4 # 5 stacks of 4
    culture_supply_vol = 25 # mL
    inducer_vol = 200 # uL
    max_transfer_vol = 985 # uL
    rinse_mix_cycles = 4
    rinse_replacements = 5
    cycle_replace_vol = 250 # uL
    read_sample_vol = 175 # uL
    assert read_sample_vol < cycle_replace_vol
    generation_time = 30 * 60 # seconds
    fixed_lagoon_height = 19 # mm for 500uL lagoons
    lagoon_fly_disp_height = fixed_lagoon_height + 18 # mm
    wash_vol = max_transfer_vol # uL

    layfile = os.path.join(this_file_dir, 'assets', 'deck.lay')
    lmgr = LayoutManager(layfile)

    # deck locations
    lagoon_plate = lmgr.assign_unused_resource(ResourceType(Plate96, 'lagoons'))
    plate_trash = lmgr.assign_unused_resource(ResourceType(Plate96, 'plate_trash'))
    culture_reservoir = lmgr.assign_unused_resource(ResourceType(Plate96, 'waffle'))
    reader_tray = lmgr.assign_unused_resource(ResourceType(Plate96, 'reader_tray'))
    inducer_site = lmgr.assign_unused_resource(ResourceType(Plate96, 'inducer'))
    bleach_site = lmgr.assign_unused_resource(ResourceType(Tip96, 'RT300_HW_96WashDualChamber1_bleach'))
    rinse_site = lmgr.assign_unused_resource(ResourceType(Tip96, 'RT300_HW_96WashDualChamber1_water'))
    inducer_tips = lmgr.assign_unused_resource(ResourceType(Tip96, 'inducer_tips'))
    
    # tip areas
    tip_staging = lmgr.assign_unused_resource(ResourceType(Tip96, 'tip_staging'))
    
    # individual tip boxes
    l_middle_back = lmgr.assign_unused_resource(ResourceType(Tip96, 'l_middle_back'))
    l_middle = lmgr.assign_unused_resource(ResourceType(Tip96, 'l_middle'))
    l_middle_front = lmgr.assign_unused_resource(ResourceType(Tip96, 'l_middle_front'))
    l_front = lmgr.assign_unused_resource(ResourceType(Tip96, 'l_front'))
    r_back = lmgr.assign_unused_resource(ResourceType(Tip96, 'r_back'))
    r_middle_back = lmgr.assign_unused_resource(ResourceType(Tip96, 'r_middle_back'))
    r_middle = lmgr.assign_unused_resource(ResourceType(Tip96, 'r_middle'))
    r_middle_front = lmgr.assign_unused_resource(ResourceType(Tip96, 'r_middle_front'))
    #r_front = lmgr.assign_unused_resource(ResourceType(Tip96, 'r_front'))
    
    # individual plate reader plates
    rp_l_middle_back = lmgr.assign_unused_resource(ResourceType(Plate96, 'rp_l_middle_back'))
    rp_l_middle = lmgr.assign_unused_resource(ResourceType(Plate96, 'rp_l_middle'))
    rp_l_middle_front = lmgr.assign_unused_resource(ResourceType(Plate96, 'rp_l_middle_front'))
    rp_l_front = lmgr.assign_unused_resource(ResourceType(Plate96, 'rp_l_front'))
    
    inducer_tip_pos_gen = iter(zip([inducer_tips] * 96, range(96)))
    
    # define system state
    sys_state = types.SimpleNamespace()
    sys_state.need_to_refill_washer = True
    sys_state.need_to_read_plate = False
    sys_state.mounted_tips = None
    
    sys_state.disable_pumps = '--no_pumps' in sys.argv
    debug = '--debug' in sys.argv
    simulation_on = debug or '--simulate' in sys.argv

    def clean_reservoir(pump_int, shaker):
        shaker.start(300)
        pump_int.bleach_clean()
        shaker.stop()

    shaker = Shaker()
    with HamiltonInterface(simulate=simulation_on) as ham_int, LBPumps() as pump_int, ClarioStar() as reader_int:
        if sys_state.disable_pumps or simulation_on:
            pump_int.disable()
        if simulation_on:
            reader_int.disable()
            shaker.disable()
        ham_int.set_log_dir(os.path.join(local_log_dir, 'hamilton.log'))
        logging.info('\n##### Priming pump lines and cleaning reservoir.')
        
        prime_and_clean = run_async(lambda: (#pump_int.prime(),             # important that the shaker is
                shaker.start(300), pump_int.bleach_clean(),
                shaker.stop())) # started and stopped at least once
        
        
        initialize(ham_int)
        hepa_on(ham_int, simulate=int(simulation_on))
        method_start_time = time.time()
        try:
            # make sure we initially have something to dispense frothy waste into
            if not sys_state.disable_pumps:
                wash_empty_refill(ham_int, refillAfterEmpty=3,  # 3=Refill chamber 2 only, which is BLEACH
                                           chamber2WashLiquid=0)    # 0=Liquid 1 (red container) (bleach)
            prime_and_clean.join()

            def clean_reservoir(pump_int, shaker):
                shaker.start(300)
                pump_int.bleach_clean()
                shaker.stop()

            # tuples of the form (tip box, offset, plate reader plate,  ___)
            tip_rotation_list = [(l_middle_back,    0,      None),
                                 (l_middle_back,    48,     rp_l_middle_back),
                                 (l_middle,         0,      None),
                                 (l_middle,         48,     None),
                                 (l_middle_front,   0,      None),
                                 (l_middle_front,   48,     rp_l_middle),
                                 (l_front,          0,      None),
                                 (l_front,          48,     None),
                                 (r_back,           0,      None),
                                 (r_back,           48,     rp_l_middle_front),
                                 (r_middle_back,    0,      None),
                                 (r_middle_back,    48,     None),
                                 (r_middle,         0,      None),
                                 (r_middle,         48,     rp_l_front),
                                 (r_middle_front,   0,      None),
                                 (r_middle_front,   48,     None),
                                 ]
            
            rotation_variable = 0
            while(True):
                start_time = time.time()
                (tip_rotation_box, tip_rotation_offset, reader_plate_site) = tip_rotation_list[rotation_variable]
                
                plate_read = True
                if reader_plate_site is None:
                    plate_read = False
                
                logging.info('\n##### Tip rotation this iteration: ' + str(rotation_variable) + ' ' + str(tip_rotation_box.layout_name()) + ' ' + str(tip_rotation_offset))
                rotation_variable = (rotation_variable + 1) % len(tip_rotation_list)
              
                # summon bacterial culture
                culture_fill_thread = run_async(lambda: (pump_int.refill(culture_supply_vol), shaker.start(250), time.sleep(30))) # start shaker asap to mix in inducer, mix for at least 30 seconds

                # dispense inducer in an x-pattern into the reservoir
                logging.info('\n##### Filling reservoir and adding inducer.')
                
                # use 8-channel to fetch a tip to put in the tip staging
                # pick up tip with 8-channel
                while True:
                    try:
                        tip_pick_up(ham_int, [next(inducer_tip_pos_gen)])
                        break
                    except pyhamilton.NoTipError:
                        continue
                # put in tip staging
                tip_eject(ham_int, [(tip_staging, 95)])
                
                # pick up inducer tip with 96-head
                tip_pick_up_96(ham_int, tip_staging)
                aspirate_96(ham_int, inducer_site, inducer_vol)
                dispense_96(ham_int, culture_reservoir, inducer_vol)
                tip_eject_96(ham_int)

                ## With 96-head, perform culture swap
                # move the next batch of 48 tips to tip_staging using the 8-channel head
                for i in range(6):
                    tip_pick_up(ham_int, [(tip_rotation_box, x+i*8 + tip_rotation_offset) for x in range(8)])
                    tip_eject  (ham_int, [(tip_staging     , x+i*2*8                    ) for x in range(8)])
                 
                # pick up 48-tips
                culture_fill_thread.join()
                shaker.stop()
                tip_pick_up_96(ham_int, tip_staging)

                # mix and aspirate culture
                aspirate_96(ham_int, culture_reservoir, cycle_replace_vol, mixCycles=12, mixVolume=100, liquidHeight=.5, airTransportRetractDist=15)            
                
                # dispense into lagoons
                dispense_96(ham_int, lagoon_plate, cycle_replace_vol, liquidHeight=lagoon_fly_disp_height, dispenseMode=9)
                
                ## Mix. Aspirate waste
                #aspirate_96(ham_int, lagoon_plate, cycle_replace_vol, mixCycles=2, mixPosition=2,
                #        mixVolume=cycle_replace_vol, liquidFollowing=1, liquidHeight=fixed_lagoon_height, airTransportRetractDist=30)
                #dispense_96(ham_int, reader_plate_site, read_sample_vol, liquidHeight=5, dispenseMode=9, airTransportRetractDist=30) # mode: blowout
                #
                #subsequent_dispense = cycle_replace_vol 
                ## dispense waste into plate reader plate
                #if (plate_read):
                #    
                #    dispense_96(ham_int, reader_plate_site, read_sample_vol)
                #    subsequent_dispense = cycle_replace_vol - read_sample_vol   # reduce how much waste you eject to trash
                #
                ## dispense into bleach bath
                #dispense_96(ham_int, bleach_site, subsequent_dispense)
                
                # Mix. Aspirate waste. dispense waste into plate reader plate
                aspirate_96(ham_int, lagoon_plate, read_sample_vol, mixCycles=2, mixPosition=2,
                        mixVolume=cycle_replace_vol, liquidFollowing=1, liquidHeight=fixed_lagoon_height)
                if (plate_read):
                    dispense_96(ham_int, reader_plate_site, read_sample_vol, liquidHeight=5, dispenseMode=9) # mode: blowout
                else:
                    dispense_96(ham_int, lagoon_plate, read_sample_vol, liquidHeight=fixed_lagoon_height+3, dispenseMode=9) # mode: blowout

                # draining lagoons to constant height
                excess_vol = cycle_replace_vol*1.2
                aspirate_96(ham_int, lagoon_plate, excess_vol, liquidHeight=fixed_lagoon_height)
                dispense_96(ham_int, bleach_site, excess_vol, liquidHeight=10, dispenseMode=9) # mode: blowout
                
                # start cleaning waffle /after/ waste dispense is done
                waffle_clean_thread = run_async(lambda: (pump_int.empty(culture_supply_vol), clean_reservoir(pump_int, shaker)))
                
                # trash tips
                tip_eject_96(ham_int, tipEjectToKnownPosition=2)    #2 is default waste
                                           
                # read plate
                if (plate_read):                
                    protocols = ['kinetic_supp_3_high', 'kinetic_supp_abs']
                    data_types = ['lum', 'abs']
                    platedatas = read_plate(ham_int, reader_int, reader_tray, reader_plate_site, protocols, plate_id='plate'+str(rotation_variable))
                    if simulation_on:
                        platedatas = [PlateData(os.path.join('assets', 'dummy_platedata.csv'))] * len(protocols) # sim dummies
                    for platedata, data_type in zip(platedatas, data_types):
                        platedata.wait_for_file()
                        db_add_plate_data(platedata, data_type, reader_plate_site, lagoons, [x+y*2*8 for y in range(6) for x in range(8)]) # DANGER lagoons spaced out!
                
                    # refill bleach waste every other iteration
                    if not sys_state.disable_pumps:
                        wash_empty_refill(ham_int, refillAfterEmpty=3,  # 3=Refill chamber 2 only, which is BLEACH
                                           chamber2WashLiquid=0)    # 0=Liquid 1 (red container) (bleach)
                                           
                # join all threads
                waffle_clean_thread.join()
                
                # wait remainder of cycle time
                if not simulation_on:
                    time.sleep(max(0, generation_time - time.time() + start_time))
        except Exception as e:
            errmsg_str = e.__class__.__name__ + ': ' + str(e).replace('\n', ' ')
            logging.exception(errmsg_str)
            print(errmsg_str)
        finally:
            shaker.stop()
            if not simulation_on and time.time() - method_start_time > 3600*2:
                summon_devteam('Robot thROWING ERROR! ' + __file__ + ' halted... D:',
                "Hamilton Starlet Error.\n\n" +
                ('The following exception message might help you anyway: \n\n' + errmsg_str + '\n\n' if errmsg_str else ' ') +
                "\n\n")
            