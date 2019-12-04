import numpy as np
from scipy.signal import firwin, filtfilt



def fir_filter(signal: np.ndarray,
               lowcut: int = 3,
               highcut: int = 45,
               sample_rate: int = 256) -> np.ndarray:
    """
    Filter an ECG signal with a type 1 linear phase FIR filter using the
    Hamming window method. Apply filter forwards and backward to achieve zero
    phase.

    A highpass of ~0.5-3 Hz is common, and lowpass of 30-50 Hz.

    Approach adapted from BioSPPy. See `ecg` function in:
    https://github.com/PIA-Group/BioSPPy/blob/master/biosppy/signals/ecg.py
    """
    order = int(0.3 * sample_rate)
    if order % 2 == 0:
        order += 1  # Ensure order is odd to get Type 1 filter

    frequency = np.array([lowcut, highcut], dtype='float')
    frequency = 2. * frequency / float(sample_rate)  # Get Nyquist frequency

    a = np.array([1])
    b = firwin(numtaps=order, cutoff=frequency, pass_zero=False)

    return filtfilt(b, a, signal)
