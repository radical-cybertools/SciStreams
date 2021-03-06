import matplotlib as mpl

# Custom colormaps
# ROYGBVR but with Cyan-Blue instead of Blue
color_list_cyclic_spectrum = [
    [1.0, 0.0, 0.0],
    [1.0, 165.0/255.0, 0.0],
    [1.0, 1.0, 0.0],
    [0.0, 1.0, 0.0],
    [0.0, 0.2, 1.0],
    [148.0/255.0, 0.0, 211.0/255.0],
    [1.0, 0.0, 0.0]
]
cmap_cyclic_spectrum = mpl.colors\
        .LinearSegmentedColormap\
        .from_list('cmap_cyclic_spectrum', color_list_cyclic_spectrum)

# classic jet, slightly tweaked
# (bears some similarity to mpl.cm.nipy_spectral)
color_list_jet_extended = [
    [0.00000000, 0.00000000, 0.00000000],
    [0.18000000, 0.00000000, 0.18000000],
    [0.00000000, 0.00000000, 0.50000000],
    [0.00000000, 0.00000000, 1.00000000],
    [0.00000000, 0.38888889, 1.00000000],
    [0.00000000, 0.83333333, 1.00000000],
    [0.30465950, 1.00000000, 0.66308244],
    [0.66308244, 1.00000000, 0.30465950],
    [1.00000000, 0.90123457, 0.00000000],
    [1.00000000, 0.48971193, 0.00000000],
    [1.00000000, 0.07818930, 0.00000000],
    [1.00000000, 0.00000000, 0.00000000],
    [0.50000000, 0.00000000, 0.00000000],
]
cmap_jet_extended = mpl.colors\
        .LinearSegmentedColormap\
        .from_list('cmap_jet_extended', color_list_jet_extended)

# Tweaked version of "view.gtk" default color scale
color_list_vge = [
    [0.0/255.0, 0.0/255.0, 0.0/255.0],
    [0.0/255.0, 0.0/255.0, 254.0/255.0],
    [188.0/255.0, 2.0/255.0, 107.0/255.0],
    [254.0/255.0, 55.0/255.0, 0.0/255.0],
    [254.0/255.0, 254.0/255.0, 0.0/255.0],
    [254.0/255.0, 254.0/255.0, 254.0/255.0]
]
cmap_vge = mpl.colors\
        .LinearSegmentedColormap\
        .from_list('cmap_vge', color_list_vge)

# High-dynamic-range (HDR) version of VGE
color_list_vge_hdr = [
    [255.0/255.0, 255.0/255.0, 255.0/255.0],
    [0.0/255.0, 0.0/255.0, 0.0/255.0],
    [0.0/255.0, 0.0/255.0, 255.0/255.0],
    [188.0/255.0, 0.0/255.0, 107.0/255.0],
    [254.0/255.0, 55.0/255.0, 0.0/255.0],
    [254.0/255.0, 254.0/255.0, 0.0/255.0],
    [254.0/255.0, 254.0/255.0, 254.0/255.0]
]
cmap_vge_hdr = mpl.colors\
        .LinearSegmentedColormap\
        .from_list('cmap_vge_hdr', color_list_vge_hdr)

# Simliar to Dectris ALBULA default color-scale
color_list_hdr_albula = [
    [255.0/255.0, 255.0/255.0, 255.0/255.0],
    [0.0/255.0, 0.0/255.0, 0.0/255.0],
    [255.0/255.0, 0.0/255.0, 0.0/255.0],
    [255.0/255.0, 255.0/255.0, 0.0/255.0],
    # [ 255.0/255.0, 255.0/255.0, 255.0/255.0],
]
cmap_hdr_albula = mpl.colors\
        .LinearSegmentedColormap\
        .from_list('cmap_hdr_albula', color_list_hdr_albula)

# Ugly color-scale, but good for highlighting many features in HDR data
color_list_cur_hdr_goldish = [
    [255.0/255.0, 255.0/255.0, 255.0/255.0],  # white
    [0.0/255.0, 0.0/255.0, 0.0/255.0],  # black
    [100.0/255.0, 127.0/255.0, 255.0/255.0],  # light blue
    [0.0/255.0, 0.0/255.0, 127.0/255.0],  # dark blue
    # [ 0.0/255.0, 127.0/255.0, 0.0/255.0],  # dark green
    [127.0/255.0, 60.0/255.0, 0.0/255.0],  # orange
    [255.0/255.0, 255.0/255.0, 0.0/255.0],  # yellow
    [200.0/255.0, 0.0/255.0, 0.0/255.0],  # red
    [255.0/255.0, 255.0/255.0, 255.0/255.0],  # white
]
cmap_hdr_goldish = mpl.colors\
        .LinearSegmentedColormap\
        .from_list('cmap_hdr_goldish', color_list_cur_hdr_goldish)
