from setuptools import setup

mmtfPyspark_packages = ['mmtfPyspark',
                        'mmtfPyspark.datasets',
                        'mmtfPyspark.filters',
                        'mmtfPyspark.io',
                        'mmtfPyspark.mappers',
                        'mmtfPyspark.ml',
                        'mmtfPyspark.utils',
                        'mmtfPyspark.webFilters',
                        'mmtfPyspark.webServices',
                        'mmtfPyspark.interactions'
                        ]

mmtfPyspark_dependencies = ['pyspark',
                            'Biopython',
                            'msgpack-python',
                            'numpy',
                            'ipywidgets',
                            'mmtf-python',
                            'sympy',
                            'requests'
                            ]

setup(name='mmtfPyspark',
      version='0.2.0',
      description='Methods for parallel and distributed analysis and mining of the Protein Data Bank using MMTF and Apache Spark',
      url='https://github.com/sbl-sdsc/mmtf-pyspark',
      author='Mars Huang (Shih-Cheng)',
      author_email='marshuang80@gmail.com',
      license='Apache License 2.0',
      keywords='mmtf spark pyspark protein PDB',
      packages=mmtfPyspark_packages,
      install_requires=mmtfPyspark_dependencies,
      python_requires='>=3.6',
      include_package_data=True,
      test_suite='nose.collector',
      test_require=['nose'],
      classifiers=['Development Status :: 3 - Alpha',
                 'Intended Audience :: Science/Research',
                 'Topic :: Scientific/Engineering :: Bio-Informatics',
                 'License :: OSI Approved :: Apache Software License',
                 'Programming Language :: Python :: 3.6'],
      zip_safe=False)
