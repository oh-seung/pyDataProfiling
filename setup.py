
import setuptools

setuptools.setup(name='pyDataProfiling',
		version='0.0.1',
		description='Data Profiling Package(Oracle, Tibero Only)',
		author='oh seung',
		author_email='seung.oh17@gmail.com',
		url='https://github.com/oh-seung/pyDataProfiling',
		python_requires='>=3.8',
		include_package_data=True,
		packages = setuptools.find_packages(),
		install_requires= ['pandas>=1.3', 
				   'numpy>=1.20',
				   'tqdm>=4.62',
           'cx-Oracle>=8.3.0',
           'pyodbc>=4.0.32', ]
    )
