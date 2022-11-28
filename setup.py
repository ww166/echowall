from setuptools import find_packages, setup

__version__ = "2.6.7"

setup(
    name="echowall",
    version=__version__,
    license='MIT',
    author='ww166',
    author_email='ww166mail@gmail.com',
    long_description=open('ReadMe.md', encoding='utf-8').read(),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=["echowall.tests", ".vscode"]),
    install_requires=["rocketmq-client-python==2.0.0",
                      "snowflake-id==0.0.2"],
    keywords='RocketMQ,MessageQueue',
    py_modules=["setup", "echowall", ],
    project_urls={
        'GitHub': 'https://github.com/ww166/echowall',
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Documentation',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
