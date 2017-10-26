#!/usr/bin/env python
import numpy as np

# from distutils.core import setup
import setuptools

setuptools.setup(name='SciStreams',
                 version='0.2',
                 author='Kevin Yager',
                 description="CMS Analysis",
                 include_dirs=[np.get_include()],
                 author_email='lhermitte@bnl.gov',
                 # install_requires=['six', 'numpy'],  # essential deps only
                 keywords='CMS X-ray Analysis',
                 )
