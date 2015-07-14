''' pybq.bqviz: helper functions that support plotting functionality of BQDF
'''

import numpy as np
import matplotlib.pyplot as plt

def _get_summaries(plotdf, value_col, grouping_col):
    '''generates labels, means, and sems for plotting'''
    labels = plotdf[grouping_col].values
    means = plotdf[value_col + '_mean'].values
    stds = plotdf[value_col + '_std'].values
    counts = plotdf[value_col + '_count'].values
    sems = [std / np.sqrt(count) for std, count in zip(stds, counts)]
    return labels, means, sems


def _plot_grouped_data(plotdf, value_col, grouping_col, kind='bar'):
    '''plots data from value_col (Y), grouped by grouping_col (X)'''
    labels, means, sems = _get_summaries(plotdf, value_col, grouping_col)
    f, ax = plt.subplots(figsize=[14, 3])
    xaxis = range(len(means))
    if kind == 'line':
        ax.errorbar(xaxis, means, yerr=sems)
    else:
        ax.bar(xaxis, means, yerr=sems)
    ax.set_xlim([min(xaxis), max(xaxis)])
    ax.set_xticklabels(labels, rotation=90)
    return ax
