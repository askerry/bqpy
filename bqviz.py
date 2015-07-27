''' pybq.bqviz: helper functions that support plotting functionality of BQDF
'''

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


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


def plot_hist(df, col, ax=None):
    '''plot histogram of values in col'''
    labels = ['<' + val[1:] for val in df.T.index.values]
    freqs = df.T.iloc[:, 0].values
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 4])
    ax.bar(range(len(freqs)), freqs)
    ax.set_xticklabels(labels, rotation=90)
    ax.set_ylabel('frequency')
    ax.set_title("%s histogram" % col)


def plot_scatter(df, xcol, ycol, ax=None, downsampled=True, error=None, counts=None):
    '''plot a scatter plot of the data in xcol vs. ycol
    INPUTS:
       df (pandas dataframe): contains x and y datapoints to plot
       xcol (str): name of x column
       ycol (str): name of y column
       ax (plt axis): optional axis to plot on
       downsampled (bool): specifies whether we are plotting all the datapoints or summaries of different bins (downsampled=True)
       error (list/array): standard error to plot if plotting downsampled summaries
       counts (list/array): number of datapoints contributing to each marker if plotting downsampled summaries (used to size markers)
    OUPUTS:
       plotdf (pandas dataframe): contains all data used to create plot
    '''
    if ax is None:
        f, ax = plt.subplots(figsize=[8, 4])
    if downsampled:
        x = [float(val[1:]) for val in df.columns]
        y = df.T[0].values
        minc, maxc = min(counts), max(counts)
        normalized_counts = [
            300 * (float((count - minc)) / (maxc - minc)) for count in counts]
        plt.scatter(x, y, marker='o', s=normalized_counts)
        plt.errorbar(x, y, yerr=error, ecolor='#6699ff', linestyle="None")
        ax.set_ylabel(ycol + " +/- SEM")
    else:
        x = df[xcol]
        y = df[ycol]
        ax.scatter(x, y)
        ax.set_ylabel(ycol)
    ax.set_xlabel(xcol)
    if downsampled:
        ax.set_title("downsampled to %s bins" % len(x))
        return pd.DataFrame(index=x, data={'mean_' + ycol: y, 'std_' + ycol: error, 'count' + ycol: counts})
    else:
        ax.set_title("all data shown")
        return pd.DataFrame(index=x, data={ycol: y})
