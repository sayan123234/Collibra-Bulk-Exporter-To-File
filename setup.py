from setuptools import setup, find_packages

setup(
    name="collibra-bulk-exporter",
    version="1.0.0",
    description="Bulk export assets from Collibra",
    author="Sayan",
    author_email="sayan123234@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "pandas",
        "python-dotenv",
        "openpyxl",  # For Excel support
    ],
    entry_points={
        "console_scripts": [
            "collibra-exporter=main:main",
        ],
    },
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
