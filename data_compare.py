def data_comparsion(src_meta,trg_meta,formatted_src_data,formatted_trg_data,IgnoreTrailingSpace):
    match_fields = [value for value in src_meta.keys() if value in trg_meta.keys()]
    match_key_fields = [value for value in formatted_src_data.keys() if value in formatted_trg_data.keys()]
    src_rec_num = len(formatted_src_data.keys())
    trg_rec_num = len(formatted_trg_data.keys())
    matched_rec_num = len(match_key_fields)
    unused_src = src_rec_num-matched_rec_num
    unused_trg = trg_rec_num-matched_rec_num
    data_comp = {}
    data_samp = {}
#   
    for pri_key in match_key_fields:
        flag = 0
#
        for field in match_fields:
#
            srcdatatyp = src_meta[field]["Datatype"]
            tgtdatatyp = trg_meta[field]["Datatype"]
#
            if IgnoreTrailingSpace =='Y' and srcdatatyp.upper() in ('CHAR','VARCHAR') and tgtdatatyp.upper() in ('CHAR','VARCHAR') and formatted_src_data[pri_key][field] != None and formatted_trg_data[pri_key][field] != None:
                checkResult = formatted_src_data[pri_key][field].rstrip() != formatted_trg_data[pri_key][field].rstrip()
            else:
                checkResult = formatted_src_data[pri_key][field] != formatted_trg_data[pri_key][field]
#
            if checkResult:
#
                if field in data_comp.keys():
                    data_comp[field] += 1
                else:
                    data_comp[field] = 1
#
                data_samp[field] = [formatted_src_data[pri_key][field],formatted_trg_data[pri_key][field],pri_key]
                flag = 1
#
        if flag == 1:
            matched_rec_num = matched_rec_num-1
            unused_src = unused_src+1
            unused_trg = unused_trg+1
#
    data_ovr = [src_rec_num,trg_rec_num,matched_rec_num,unused_src,unused_trg]
    data_comp_res = [data_ovr,data_comp,data_samp]
    return data_comp_res