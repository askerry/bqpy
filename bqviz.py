import numpy as np
import matplotlib.pyplot as plt

def get_summaries(plotdf, value_col, grouping_col):
    labels = plotdf[grouping_col].values
    means = plotdf[value_col + '_mean'].values
    stds = plotdf[value_col + '_std'].values
    counts = plotdf[value_col + '_count'].values
    sems = [std / np.sqrt(count) for std, count in zip(stds, counts)]
    return labels, means, sems


def plot_grouped_data(plotdf, value_col, grouping_col, kind='bar'):
    labels, means, sems = get_summaries(plotdf, value_col, grouping_col)
    f, ax = plt.subplots(figsize=[14, 3])
    xaxis = range(len(means))
    if kind == 'line':
        ax.errorbar(xaxis, means, yerr=sems)
    else:
        ax.bar(xaxis, means, yerr=sems)
    ax.set_xlim([min(xaxis), max(xaxis)])
    ax.set_xticklabels(labels, rotation=90)
    return ax
