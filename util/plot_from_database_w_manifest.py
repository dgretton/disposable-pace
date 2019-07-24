import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
import pdb
import sys
import os
import csv
import matplotlib.patches as mpatches
import matplotlib

number_of_wells = 96

def plot_well(well, type, plt, color = 'b', linewidth = 1, linestyle = '.-'):
    '''Fetches data of type (lum, abs) for a particular well and plots it on the graph'''
    n = (well, type, )
    c.execute('SELECT filename, well, reading FROM measurements WHERE well=? AND data_type=?', n)

    x = c.fetchall()
    print(len(x), "entries fetched")
    vals = [(datetime.strptime(f[-15:-4], '%y%m%d_%H%M'), w, v) for (f, w, v) in x if 'dummy' not in f]
    vals = [(t, w, v) for t, w, v in vals if t > datetime(2019, 2, 27, 7, 37)] 

    # cut out bad datapoints
    remove_datetimes = [datetime(2019, 2, 27, 14, 58), datetime(2019, 2, 27, 11, 57), datetime(2019, 2, 27, 10, 57), datetime(2019, 2, 26, 18, 11), datetime(2019, 2, 27, 7, 37)]
    vals = [(t, w, v) for (t, w, v) in vals if t not in remove_datetimes]
    
    # downsample to just 23 hours
    vals = vals[:13]
    
    plt.plot([j for (j, _, _) in vals], [lum for (j, _, lum) in vals], color = color, linestyle = linestyle, linewidth=linewidth, marker = 'o', markersize = 12)
    
    # decrease number of plotted X axis labels
    # make there be fewer labels so that you can read them
    times = [x for (x, _, _) in vals]
    deltas = [t - times[0] for t in times]
    labels = [int(d.seconds/60/60 + d.days*24) for d in deltas]
    labels_sparse = [labels[x] if x % 6 == 0 else '' for x in range(len(labels))]
    plt.xticks(times, labels_sparse)
    locs, labels = plt.xticks()

# automatically find the database file for this 96-robot method
db_dir = os.path.join('..', 'method_local')

if len(sys.argv) > 2:
    print('Only (optional) argument is the name of the database you want to plot from')
    exit()
dbs = [filename for filename in os.listdir(db_dir) if filename.split('.')[-1] == 'db']
if len(sys.argv) == 2:
    db_name = sys.argv[1]
    if db_name not in dbs:
        print('database does not exist in ' + db_dir)
        exit()
else:
    if len(dbs) != 1:
        print('can\'t infer which database you want to plot from, please specify with argument')
        exit()
    db_name, = dbs

conn = sqlite3.connect(os.path.join(db_dir, db_name))
c = conn.cursor()

# read in the manifest file
# assign colors to the types as they arise
colors = [name for name,hex in matplotlib.colors.cnames.items()]

# read in manifest file
csvfile = open('Manifest.csv', 'r')
reader = csv.reader(csvfile)
manifest = {}
well_phage = {}
for row in reader:
    (plate, well, type, phage) =  row[:4]
    
    if type not in manifest:
        manifest[type] = [well]
    else:
        manifest[type].append(well)
    well_phage[well] = type

'''
# 96-plot
for measurement_type in ['lum', 'abs']:
    scale = 2
    fig1 = plt.figure(figsize=(24*scale, 16*scale))
    subplot = 0
    for column in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
        for row in range(1,13):
            subplot = subplot + 1
            well = column + str(row)

            # set up plot
            ax = fig1.add_subplot(8, number_of_wells/8, subplot)
            ax.set_title("Lagoon" + str(well) + ': ' + well_phage[well], x=0.5, y=0.8)

            # did this well have phage? if so, plot in red
            color = 'r'
            if well_phage[well] == "no phage control":
                color = 'b'
            
            # plot a single well
            plot_well(well, measurement_type, plt, color = color, linestyle = 'solid', linewidth = 7)
    
            # adjust limit values to reflect the type of graph
            if measurement_type == 'abs':
                plt.ylim(0.0, 1.0)
            else:
                ax.set_yscale('log')
                plt.ylim(200.0, 100000.0)
        
    fig1.tight_layout()
    plt.savefig(os.path.join(db_dir, 'manifest_plot_' + measurement_type + ".png"), dpi = 200)
'''

# manifest plot
for measurement_type in ['lum', 'abs']:
    fig2 = plt.figure()
    
    # plot the data one type at a time
    i = 0
    patches = []
    for type, wells in list(manifest.items()):
        i = i + 1
        
        # plot one well at a time
        for well in wells:
            ls = 'solid'
            if well_phage[well] == "no phage control":
                ls = 'dashed'
            plot_well(well, measurement_type, plt, color = colors[i%len(colors)], linestyle = ls, linewidth = 2)
    
        # add the legend item
        patches.append(mpatches.Patch(color=colors[i%len(colors)], label=type))

    # plot the legend
    #plt.legend(handles=patches, loc='upper left')
    plt.legend(handles=patches, bbox_to_anchor=(1,-.6), loc="lower right", 
                bbox_transform=fig2.transFigure, ncol=3)

    plt.xlabel("Hours")
    plt.ylabel(measurement_type)
    if measurement_type == 'lum':
        plt.title("Luminescence monitoring")
    else:
        plt.title("Absorbance monitoring")
        
    # adjust limit values to reflect the type of graph
    if measurement_type == 'abs':
        plt.ylim(0.2, 0.6)
    else:
        #plt.ylim(350.0, 4000.0)
        fig2.get_axes()[0].set_yscale('log')
        plt.autoscale(enable=True, axis='y')
    
    #fig2.tight_layout()
    plt.savefig(os.path.join(db_dir, 'manifest_single_plot_' + measurement_type + ".png"), dpi = 200, bbox_inches="tight")

conn.close()