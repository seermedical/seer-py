# Copyright 2017 Seer Medical Pty Ltd, Inc. or its affiliates. All Rights Reserved.
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
#import seaborn as sns
#sns.set_style("white", {"axes.edgecolor": "0.99"})


def seerPlot(x, y=False, figSize=(15,2)):
    channelLabels = []
    
    if type(x) == pd.core.series.Series:
        x = pd.DataFrame(x)
    
    if type(x) == pd.core.frame.DataFrame:
        channelLabels = x.columns.values.tolist()
        x = -x.as_matrix().T  
    elif type(x) == np.ndarray:
        x = -x.T
    else:
        print('seerPlot accepts numpy arrays and pandas dataframes only')
    
    if type(y) == np.ndarray:
        y = y.astype(np.float32)
        
    if len(x.shape) == 1:
        x = x.reshape(1,-1)
    
    channels = x.shape[0]
    figSize = (figSize[0], figSize[1] * channels)
    l = np.arange(x.shape[1])
    fig, axs = plt.subplots(x.shape[0], 1, sharex=True, figsize=figSize, squeeze=False)
    
    fig.subplots_adjust(hspace=0)
    for k in range(channels):
        axs[k, 0].set_ylim(np.min(x[k, :])*1.1-0.1, np.max(x[k, :])*1.1+0.1)
        axs[k, 0].plot(l, x[k, :], color='#1A366D', linewidth=0.5)
        if type(y) == np.ndarray:
            lk = l.copy().astype(np.float32)
            lk[y==0] = np.nan
            xk = x[k, :].copy()
            xk[y==0] = np.nan
            axs[k, 0].plot(lk, xk, color='#EE3A20', linewidth=0.6)
        axs[k, 0].set(yticks=[])
        if len(channelLabels)>0:
            axs[k, 0].set_ylabel(channelLabels[k])
    plt.show()
