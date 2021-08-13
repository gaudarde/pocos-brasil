#!/usr/bin/python
# -*- coding: latin-1 -*-

import dados
import download

if __name__ == '__main__':

    download.download()
    download.merge()
    dados.dados()