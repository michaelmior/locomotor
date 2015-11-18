from distutils.core import setup, Command


class PyTest(Command):
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        import sys,subprocess
        errno = subprocess.call([sys.executable, 'runtests.py', 'tests'])
        raise SystemExit(errno)


setup(
    name='locomotor',
    packages=['locomotor'],
    version='0.0.1',
    description='Some Redis benchmarking experiments',
    author='Michael Mior',
    author_email='michael.mior@gmail.com',

    cmdclass={'test': PyTest},

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
