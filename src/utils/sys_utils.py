

def multiprocessing_start_modes(mode_linux='forkserver', mode_window=None):
    import platform
    import multiprocessing as mp
    sys_str = platform.system()
    if sys_str == 'Linux':
        # Linux 并行用forksever
        if mode_linux:
            mp.set_start_method(mode_linux)
    elif sys_str == 'Windows':
        if mode_window:
            mp.set_start_method(mode_window)
    else:
        raise NotImplemented('Do not know this system: {}'.format(sys_str))
