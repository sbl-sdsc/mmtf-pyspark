'''Returns unmodifiable sets of "uninteresting" (e.g., not drug like) ligands.

References
----------
- `D3R Project <https://github.com/drugdata/D3R/blob/master/d3r/filter/filtering_sets.py`_

'''

METAL_CONTAINING = set(['CP', 'NFU', 'NFR', 'NFE', 'NFV', 'FSO', 'WCC', 'TCN', 'FS2',
                        'PDV', 'CPT', 'OEC', 'XCC', 'NFS', 'C7P', 'TBR', 'NFC', 'CUB',
                        'VA3', 'FV1', 'IME', 'FC6', 'RU7', 'TBY', 'REI', 'REJ', 'CNB',
                        'MM1', 'MM2', 'MM6', 'MM5', 'YBT', 'CN1', 'CLF', 'CLP', 'NC1',
                        'V4O', 'HC0', 'VO3', 'CFM', 'CZL', 'CON', 'TBR', 'ICS', 'HCN',
                        'CFN', 'CFC', 'HF3', 'ZRC', 'F3S', 'SRM', 'HDD', 'CUA', 'RU8',
                        'B22', 'BEF', 'AG1', 'SF4', 'NCO', '0KA', 'FNE', 'QPT'])

STABILIZERS = set(['B3P', 'PGE', '6JZ', '15P', 'PE3', 'XPE', '7PE', 'M2M',
                   '13P', '3PP', 'PX4', '3OL', 'OC9', 'AE3', '6JZ', 'XPE',
                   '211', 'ODI', 'DIA', 'PG5', 'CXE', 'ME2', 'P4G', 'TOE',
                   'PG5', 'PE8', 'ZPG', 'PE3', 'MXE'])

BUFFERS = set(['MPO', 'NHE', 'CXS', 'T3A', '3CX', '3FX', 'PIN', 'MES',
               'EPE', 'TRS', 'BTB', '144'])

COFACTORS = set(['ATP', 'ADP', 'AMP', 'ANP', 'GTP', 'GDP', 'GNP', 'UMP', 'TTP',
                 'TMP', 'MGD', 'H2U', 'ADN', 'APC', 'M2G', 'OMG', 'OMC', 'UDP',
                 'UMP', '5GP', '5MU', '5MC', '2MG', '1MA', 'NAD', 'NAP', 'NDP',
                 'FAD', 'FMN', 'BH4', 'BPH', 'BTN', 'PST', 'SAM', 'SAH', 'COA',
                 'ACO', 'U10', 'HEM', 'HEC', 'HEA', 'HAS', 'DHE', 'BCL', 'CLA',
                 '6HE', '7HE', 'DCP', '23T', 'H4B', 'WCC', 'CFN', 'AMP', 'BCL',
                 'BCB', 'CHL', 'NAP', 'CON', 'FAD', 'NAD', 'SXN', 'U', 'G',
                 'QUY', 'UDG', 'CBY', 'ST9', '25A', ' A', ' C', 'B12', 'HAS',
                 'BPH', 'BPB', 'IPE', 'PLP', 'H4B', 'PMP', 'PLP', 'TPP', 'TDP',
                 'COO', 'PQN', 'BCR', 'XAT'])

COVALENT_MODS = set(['CS1', 'MSE', 'CME', 'CSO', 'LLP', 'IAS'])

FRAGMENTS = set(['ACE', 'ACT', 'DMS', 'EOH', 'FMT', 'IMD', 'DTT', 'BME',
                 'IPA', 'HED', 'PEP', 'PYR', 'PXY', 'OXE',
                 'TMT', 'TMZ', 'PLQ', 'TAM', 'HEZ', 'DTV', 'DTU', 'DTD',
                 'MRD', 'MRY', 'BU1', 'D10', 'OCT', 'ETE',
                 'TZZ', 'DEP', 'BTB', 'ACY', 'MAE', '144', 'CP', 'UVW', 'BET',
                 'UAP', 'SER', 'SIN', 'FUM', 'MAK',
                 'PAE', 'DTL', 'HLT', 'ASC', 'D1D', 'PCT', 'TTN', 'HDA', 'PGA',
                 'XXD', 'INS', '217', 'BHL', '16D',
                 'HSE', 'OPE', 'HCS', 'SOR', 'SKM', 'KIV', 'FCN', 'TRA', 'TRC',
                 'MZP', 'KDG', 'DHK'])

EXCIPIENTS = set(['CO2', 'SE', 'GOL', 'PEG', 'EDO', 'PG4', 'C8E', 'CE9', 'BME',
                  '1PE', 'OLC', 'MYR', 'LDA', '2CV', '1PG', '12P', 'XP4',
                  'PL3', 'PE4', 'PEU', 'MPG', 'B8M', 'BOM', '2PE', 'PG0',
                  'PE5', 'PG6', 'P33', 'DTV', 'SDS', 'DTU', 'DTD', 'MRD',
                  'MRY', 'BU1', 'LHG', 'D10', 'OCT', 'LT1', 'ETE', 'BTB',
                  'PC1', 'ACT', 'ACY', '3GD', 'CDL', 'PLC', 'D1D'])

JUNK = set(['AS8', 'PS9', 'CYI', 'NOB', 'DPO', 'MDN', 'APC', 'ACP', 'LPT',
            'PBL', 'LFA', 'PGW', 'DD9', 'PGV', 'UPL', 'PEF', 'MC3', 'LAP',
            'PEE', 'D12', 'CXE', 'T1A', 'TBA', 'NET', 'NEH', 'P2N',
            'PON', 'PIS', 'PPV', 'DPO', 'PSL', 'TLA', 'SRT', '104', 'PTW',
            'ACN', 'CHH', 'CCD', 'DAO', 'SBY', 'MYS', 'XPT', 'NM2', 'REE',
            'SO4-SO4', 'P4C', 'C10', 'PAW', 'OCM', '9OD', 'Q9C', 'UMQ',
            'STP', 'PPK', '3PO', 'BDD', '5HD', 'YES', 'DIO', 'U10', 'C14',
            'BTM', 'P03', 'M21', 'PGV', 'LNK', 'EGC', 'BU3', 'R16', '4E6',
            '1EY', '1EX', 'B9M', 'LPP', 'IHD', 'NKR', 'T8X', 'AE4', 'X13',
            '16Y', 'B3P', 'RB3', 'OHA', 'DGG', 'HXA', 'D9G', 'HTG', 'B7G',
            'FK9', '16P', 'SPM', 'TLA', 'B3P', '15P', 'SPO', 'BCR', 'BCN',
            'EPH', 'SPD', 'SPN', 'SPH', 'S9L', 'PTY', 'PE8', 'D12', 'PEK'])

DO_NOT_CALL = set(['CP', 'NFU', 'NFR', 'NFE', 'NFV', 'FSO', 'WCC', 'TCN',
                   'FS2', 'PDV', 'CPT', 'OEC', 'XCC', 'NFS', 'C7P', 'TBR',
                   'NFC', 'CUB', 'VA3', 'FV1', 'IME', 'FC6', 'RU7', 'TBY',
                   'REI', 'REJ', 'CNB', 'MM1', 'MM2', 'MM6', 'MM5', 'YBT',
                   'CN1', 'CLF', 'CLP', 'V4O', 'HC0', 'VO3', 'CFM', 'CZL',
                   'CON', 'ICS', 'HCN', 'CFN', 'CFC', 'HF3', 'ZRC', 'F3S',
                   'SRM', 'HDD', 'B3P', 'PGE', '6JZ', '15P', 'PE3', 'XPE',
                   '7PE', 'M2M', '13P', '3PP', 'PX4', '3OL', 'OC9', 'AE3',
                   '211', 'ODI', 'DIA', 'PG5', 'CXE', 'ME2', 'P4G', 'TOE',
                   'PE8', 'ZPG', 'MXE', 'MPO', 'NHE', 'CXS', 'T3A', '3CX',
                   '3FX', 'PIN', 'MGD', 'NAD', 'NAP', 'NDP', 'FAD', 'FMN',
                   'BH4', 'BPH', 'BTN', 'COA', 'ACO', 'U10', 'HEM', 'HEC',
                   'HEA', 'HAS', 'DHE', 'BCL', 'CLA', '6HE', '7HE', 'H4B',
                   'BCB', 'CHL', 'SXN', 'QUY', 'UDG', 'CBY', 'ST9', '25A',
                   'B12', 'BPB', 'IPE', 'PLP', 'PMP', 'TPP', 'TDP', 'SO4',
                   'SUL', 'CL', 'BR', 'CA', 'MG', 'NI', 'MN', 'CU', 'PO4',
                   'CD', 'NH4', 'CO', 'NA', 'K', 'ZN', 'FE', 'AZI', 'CD2',
                   'YG', 'CR', 'CR2', 'CR3', 'CAC', 'CO2', 'CO3', 'CYN',
                   'FS4', 'MO6', 'NCO', 'NO3', 'SCN', 'SF4', 'SE', 'PB',
                   'AU', 'AU3', 'BR1', 'CA2', 'CL1', 'CS', 'CS1', 'CU1',
                   'AG', 'AG1', 'AL', 'AL3', 'F', 'FE2', 'FE3', 'IR',
                   'IR3', 'KR', 'MAL', 'GOL', 'MPD', 'PEG', 'EDO', 'PG4',
                   'BOG', 'HTO', 'ACX', 'CEG', 'XLS', 'C8E', 'CE9', 'CRY',
                   'DOX', 'EGL', 'P6G', 'SUC', '1PE', 'OLC', 'POP', 'MES',
                   'EPE', 'PYR', 'CIT', 'FLC', 'TAR', 'HC4', 'MYR', 'HED',
                   'DTT', 'BME', 'TRS', 'ABA', 'ACE', 'ACT', 'CME', 'CSD',
                   'CSO', 'DMS', 'EOH', 'FMT', 'GTT', 'IMD', 'IOH', 'IPA',
                   'LDA', 'LLP', 'PEP', 'PXY', 'OXE', 'TMT', 'TMZ', '2CV',
                   'PLQ', 'TAM', '1PG', '12P', 'XP4', 'PL3', 'PE4', 'PEU',
                   'MPG', 'B8M', 'BOM', 'B7M', '2PE', 'STE', 'DME', 'PLM',
                   'PG0', 'PE5', 'PG6', 'P33', 'HEZ', 'F23', 'DTV', 'SDS',
                   'DTU', 'DTD', 'MRD', 'MRY', 'BU1', 'LHG', 'D10', 'OCT',
                   'LI1', 'ETE', 'TZZ', 'DEP', 'DKA', 'OLA', 'ACD', 'MLR',
                   'POG', 'BTB', 'PC1', 'ACY', '3GD', 'MAE', 'CA3', '144',
                   '0KA', 'A71', 'UVW', 'BET', 'PBU', 'SER', 'CDL', 'CEY',
                   'LMN', 'J7Z', 'SIN', 'PLC', 'FNE', 'FUM', 'MAK', 'PAE',
                   'DTL', 'HLT', 'FPP', 'FII', 'D1D', 'PCT', 'TTN', 'HDA',
                   'PGA', 'XXD', 'INS', '217', 'BHL', '16D', 'HSE', 'OPE',
                   'HCS', 'SOR', 'SKM', 'KIV', 'FCN', 'TRA', 'TRC', 'MTL',
                   'KDG', 'DHK', 'Ar', 'IOD', '35N', 'HGB', '3UQ', 'UNX',
                   'GSH', 'DGD', 'LMG', 'LMT', 'CAD', 'CUA', 'DMU', 'PEK',
                   'PGV', 'PSC', 'TGL', 'COO', 'BCR', 'XAT', 'MOE', 'P4C',
                   'PP9', 'Z0P', 'YZY', 'LMU', 'MLA', 'GAI', 'XE', 'ARS',
                   'SPM', 'RU8', 'B22', 'BEF', 'DHL', 'HG', 'MBO', 'ARC',
                   'OH', 'FES', 'RU', 'IAS', 'QPT', 'SR'])

all_sets = [METAL_CONTAINING, STABILIZERS, BUFFERS, COFACTORS, COVALENT_MODS, \
            FRAGMENTS, EXCIPIENTS, JUNK, DO_NOT_CALL]
ALL_GROUPS = {item for subset in all_sets for item in subset}
