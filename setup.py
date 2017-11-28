from setuptools import setup

mmtfPyspark_packages = ['mmtfPyspark',
                        'mmtfPyspark.analysis',
                        'mmtfPyspark.datasets',
                        'mmtfPyspark.filters',
                        'mmtfPyspark.inputFunction',
                        'mmtfPyspark.io',
                        'mmtfPyspark.mappers',
                        'mmtfPyspark.ml',
                        'mmtfPyspark.utils',
                        'mmtfPyspark.webfilters',
                        'mmtfPyspark.webservices'
                        ]

mmtfPyspark_dependencies = ['pyspark',
                            'Biopython',
                            'msgpack-python',
                            'numpy',
                            'mmtf-python',
                            'sympy',
                            'requests'
                            ]

setup(name='mmtfPyspark',
      version='0.1',
      description='Methods for parallel and distributed analysis and mining of the Protein Data Bank using MMTF and Apache Spark',
      url='https://github.com/sbl-sdsc/mmtf-pyspark',
      author='Mars Huang (Shih-Cheng)',
      author_email='marshuang80@gmail.com',
      license='Apache License 2.0',
      keywords='mmtf spark pyspark protein PDB',
      packages=mmtfPyspark_packages,
      install_requires=mmtfPyspark_dependencies,
      include_package_data=True,
      test_suite='nose.collector',
      test_require=['nose'],
      zip_safe=False)
