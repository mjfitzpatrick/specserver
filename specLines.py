#!/usr/bin/env python

#  SPEC_LINES -- 

__authors__ = 'Mike Fitzpatrick <fitz@noao.edu>'
__version__ = 'v1.0.0'

#
# Define a set of spectral lines.
#
# This is the set of emission lines from the SDSS spZline files.
# Wavelengths are in air for lambda > 2000, vacuum for lambda < 2000.
#
# Emission Lines
_em_lines = [
    {"name" : "Ly-alpha",       "lambda" : 1215.67,  "label" : "Ly$\\alpha$"},
    {"name" : "N V 1240",       "lambda" : 1240.81,  "label" : "N V"},
    {"name" : "C IV 1549",      "lambda" : 1549.48,  "label" : "C IV" },
    {"name" : "He II 1640",     "lambda" : 1640.42,  "label" : "He II"},
    {"name" : "C III] 1908",    "lambda" : 1908.734, "label" : "C III]"},
    {"name" : "Mg II 2799",     "lambda" : 2800.315, "label" : "Mg II" },
    {"name" : "[O II] 3725",    "lambda" : 3727.092, "label" : " "},
    {"name" : "[O II] 3727",    "lambda" : 3729.875, "label" : "[O II]"}, 
    {"name" : "[Ne III] 3868",  "lambda" : 3869.857, "label" : "[Ne III]"},
    {"name" : "H-zeta",         "lambda" : 3890.151, "label" : "H$\\zeta$"},
    {"name" : "[Ne III] 3970",  "lambda" : 3971.123, "label" : "[Ne III]"},
    {"name" : "H-epsilon",      "lambda" : 3971.195, "label" : "H$\\epsilon$"}, 
    {"name" : "H-delta",        "lambda" : 4102.892, "label" : "H$\\delta$"},
    {"name" : "H-gamma",        "lambda" : 4341.684, "label" : "H$\\beta$"},
    {"name" : "[O III] 4363",   "lambda" : 4364.435, "label" : "[O III]"},
    {"name" : "He II 4685",     "lambda" : 4686.991, "label" : "He II"},
    {"name" : "H-beta",         "lambda" : 4862.683, "label" : "H$\\beta$"},
    {"name" : "[O III] 4959",   "lambda" : 4960.294, "label" : "[O III]" },
    {"name" : "[O III] 5007",   "lambda" : 5008.239, "label" : "[O III]" },
    {"name" : "He II 5411",     "lambda" : 5413.025, "label" : "He II"},
    {"name" : "[O I] 5577",     "lambda" : 5578.888, "label" : "[O I]" },
    {"name" : "[N II] 5755",    "lambda" : 5756.186, "label" : "[Ne II]" },
    {"name" : "He I 5876",      "lambda" : 5877.308, "label" : "He I" },
    {"name" : "[O I] 6300",     "lambda" : 6302.046, "label" : "[O I]" },
    {"name" : "[S III] 6312",   "lambda" : 6313.806, "label" : "[S III]" },
    {"name" : "[O I] 6363",     "lambda" : 6365.535, "label" : "[O I]" },
    {"name" : "[N II] 6548",    "lambda" : 6549.859, "label" : "[N II]" },
    {"name" : "H-alpha",        "lambda" : 6564.614, "label" : "H$\\alpha$" },
    {"name" : "[N II] 6583",    "lambda" : 6585.268, "label" : "[N II]" },
    {"name" : "[S II] 6716",    "lambda" : 6718.294, "label" : "[S II]" },
    {"name" : "[S II] 6730",    "lambda" : 6732.678, "label" : "[S II]" },
    {"name" : "[Ar III] 7135",  "lambda" : 7137.758, "label" : "[Ar III]" },]

# Absorption lines
_abs_lines = [
    {"name" : "H12",            "lambda" : 3751.22,  "label" : "H12"},
    {"name" : "H11",            "lambda" : 3771.70,  "label" : "H11"},
    {"name" : "H10",            "lambda" : 3798.98,  "label" : "H10"},
    {"name" : "H9",             "lambda" : 3836.48,  "label" : "H9"},
    {"name" : "H-zeta",         "lambda" : 3890.151, "label" : "H$\\zeta$" },
    {"name" : "K (Ca II 3933)", "lambda" : 3934.814, "label" : "K (Ca II)"},
    {"name" : "H (Ca II 3968)", "lambda" : 3969.623, "label" : "H (Ca II)"},
    {"name" : "H-epsilon",      "lambda" : 3971.195, "label" : "H$\\epsilon$"}, 
    {"name" : "H-delta",        "lambda" : 4102.892, "label" : "H$\\delta$" },
    {"name" : "G (Ca I 4307)",  "lambda" : 4308.952, "label" : "G (Ca I)"},
    {"name" : "H-gamma",        "lambda" : 4341.684, "label" : "H$\\gamma$"},
    {"name" : "H-beta",         "lambda" : 4862.683, "label" : "H$\\beta$"},
    {"name" : "Mg I 5183",      "lambda" : 5185.048, "label" : " "},
    {"name" : "Mg I 5172",      "lambda" : 5174.125, "label" : " "},
    {"name" : "Mg I 5167",      "lambda" : 5168.762, "label" : "Mg I"},
    {"name" : "D2 (Na I 5889)", "lambda" : 5891.582, "label" : " " },
    {"name" : "D1 (Na I 5895)", "lambda" : 5897.554, "label" : "D1,2 (Na I)" },
    {"name" : "H-alpha",        "lambda" : 6564.614, "label" : "H$\\alpha$"},
    ]



def airtovac(l):
    """Convert air wavelengths (greater than 2000 Å) to vacuum wavelengths. 
    """
    if l < 2000.0:
        return l;
    vac = l
    for iter in range(2):
        sigma2 = (1.0e4/vac)*(1.0e4/vac)
        fact = 1.0+5.792105e-2/(238.0185-sigma2)+1.67917e-3/(57.362-sigma2)
        vac = l*fact
    return vac
