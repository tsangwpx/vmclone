from setuptools import setup, find_packages

import vmclone

if __name__ == '__main__':
    setup(
        name='vmclone',
        version=vmclone.__version__,
        packages=find_packages(),
        entry_points={
            'console_scripts': [
                'vmclone = vmclone.__main__',
            ]
        },
        author='Aaron Tsang',
        author_email='tsangwpx@gmail.com',
        description='Clone virtual machines with ease',
        license='MIT License',
        keywords='virtual machine online cloning utilities',
    )
