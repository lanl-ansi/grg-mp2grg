============
Introduction
============

Overview
------------------------

grg-mp2grg is a python package for translating Matpower and GRG network data files.

The primary entry point of the library is :class:`grg_mp2grg.io` module, which contains the methods for bi-directional translation.


Installation
------------------------

Simply run::

    pip install grg-mp2grg


Testing
------------------------

grg-mp2grg is designed to be a library that supports other software.  
It is not immediately useful from the terminal.
However, you can test the parsing functionality from the command line with:: 

    python -m grg_mpdata.io <path to Matpower or GRG case file>

If this command is successful, you will see a translated plain text version of the network data printed to the terminal.


