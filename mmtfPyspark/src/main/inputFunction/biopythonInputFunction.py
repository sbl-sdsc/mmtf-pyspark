'''
biopythonInputFunction.py
'''

from Bio.PDB.Polypeptide import three_to_one

def biopythonInputFunction(bp_struct, mmtf_encoder):
    # Initialize structure
    total_num_bonds = 0  # TODO Can't find bond info in bioPython
    total_num_atoms = sum(1 for x in bp_struct.get_atoms())
    total_num_groups = sum(1 for x in bp_struct.get_residues())
    total_num_chains = sum(1 for x in bp_struct.get_chains())
    total_num_models = sum(1 for x in bp_struct.get_models())
    structure_id = bp_struct.id
    mmtf_encoder.init_structure(total_num_bonds = total_num_bonds,
                                total_num_atoms = total_num_atoms,
                                total_num_groups = total_num_groups,
                                total_num_chains = total_num_chains,
                                total_num_models = total_num_models,
                                structure_id = structure_id)

    # TODO Set xtal info
    # mmtf_encoder.set_xtal_info(space_group='C 2 2 21', unit_cell=[80.37000274658203, 96.12000274658203, 57.66999816894531, 90.0, 90.0, 90.0])  ## TODO

    # TODO
    # space_group, unit_cell

    # Set header info
    header = bp_struct.header

    # MMCif doesnt have header
    if len(header) != 0:
        mmtf_encoder.set_header_info(r_free = None, # TODO
                                    r_work = None, # TODO
                                    resolution = header['resolution'],
                                    title = header['name'],
                                    deposition_date = header['deposition_date'],
                                    release_date = header['release_date'],
                                    experimental_methods = header['structure_method'])

    else:
        mmtf_encoder.set_header_info(r_free = None,
                                    r_work = None,
                                    resolution = None,
                                    title = None,
                                    deposition_date = None,
                                    release_date = None,
                                    experimental_methods = None)

    # TODO Set bioassembly info
    # biop_encoder.set_bio_assembly_trans() -- needs extra biopython parsing methods for the bioassemblies

    for model in bp_struct:

        # Set model info
        mmtf_encoder.set_model_info(model_id=model.id, chain_count=len(model.child_list))

        # Set entity info
        # biop_encoder.set_entity_info(chain_indices=, sequence, description, entity_type)  ## TODO

        for chain in model:

            # Set chain info
            chain_id = chain.id
            chain_name = chain.id  ## TODO: needs to be pulled from mmcif data?
            num_groups = len(chain.child_list)
            mmtf_encoder.set_chain_info(chain_id=chain_id, chain_name=chain_name, num_groups=num_groups)

            sequence_index = 0
            for residue in chain:
                group_name = residue.resname
                resinfo = residue.get_id()
                group_number = resinfo[1]
                if resinfo[2] == ' ':
                    insertion_code = '\x00'
                else:
                    insertion_code = resinfo[2]
                group_type = "No_group_info"  ## TODO: convert chemical type to group_type integer using chemcomp dictionary
                atom_count = len(residue.child_list)
                bond_count = 0  ## TODO: how to get bond count?
                try:
                    single_letter_code = three_to_one(residue.resname)
                except KeyError:
                    single_letter_code = 'X'

                secondary_structure_type = 1  ## TODO: precalculate DSSP secondary structure

                # Set group info
                mmtf_encoder.set_group_info(group_name=group_name, group_number=group_number,
                                           insertion_code=insertion_code, group_type=group_type,
                                           atom_count=atom_count, bond_count=bond_count, single_letter_code=single_letter_code,
                                           sequence_index=sequence_index, secondary_structure_type=secondary_structure_type)
                sequence_index += 1

                for atom in residue:
                    atom_name = atom.name
                    if atom.serial_number:
                        serial_number = atom.serial_number
                    else:
                        serial_number = 1  ## TODO
                    if atom.altloc == ' ':
                        alternative_location_id = '\x00'
                    else:
                        alternative_location_id = atom.altloc
                    x = atom.coord[0]
                    y = atom.coord[1]
                    z = atom.coord[2]
                    occupancy = atom.occupancy
                    temperature_factor = atom.bfactor
                    element = atom.element
                    charge = 0  ## TODO: how to get formal charge?
                    mmtf_encoder.set_atom_info(atom_name=atom_name,
                                               serial_number=serial_number,
                                               alternative_location_id=alternative_location_id,
                                               x=x,
                                               y=y,
                                               z=z,
                                               occupancy=occupancy,
                                               temperature_factor=temperature_factor,
                                               element=element,
                                               charge=charge)

    #                 biop_encoder.set_group_bond()  ## TODO
    #                 biop_encoder.set_inter_group_bond()  ## TODO

    print(mmtf_encoder)

    mmtf_encoder.finalize_structure()
