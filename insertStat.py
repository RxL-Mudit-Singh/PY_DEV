import json
from flatten_json import flatten

# with open('jsons/msg1.json','r',encoding='utf-8') as row:
#     string = row.read()
#     obj = json.loads(string)
#     # for msg in obj:
#     print(type(obj))
#     print(flatten(obj))
    # c_actions_addl
    # for i in range(len(obj['C_ACTIONS_ADDL']['ArrayElem'])):
    #         f"""insert into C_ACTIONS_ADDL VALUES({obj['TENANT_ID']},{obj['CASE_ID']},{obj['VERSION_NUM']},
    #         {obj['C_ACTIONS_ADDL']['ArrayElem'][i]['CMT_REC_NUM']},{obj['C_ACTIONS_ADDL']['ArrayElem'][i]['SYSTEM_CREATED']},
    #         {obj['C_ACTIONS_ADDL']['ArrayElem'][i]['DELETED_FLAG']},{obj['C_ACTIONS_ADDL']['ArrayElem'][i]['DATE_VERSION_END']},
    #         {obj['C_ACTIONS_ADDL']['ArrayElem'][i]['DELETED']},{obj['C_ACTIONS_ADDL']['ArrayElem'][i]['DATE_VERSION_START']},
    #         {obj['C_ACTIONS_ADDL']['ArrayElem'][i]['SENT_IN_LTR']})"""
    # # print("---------------------------------------------------------------------------")
    # # c_ae_identification
    # for i in range(len(obj['C_AE_IDENTIFICATION']['ArrayElem'])):
    #         f"""insert into C_ACTIONS_ADDL VALUES
    #         ({obj['TENANT_ID']},{obj['CASE_ID']},{obj['VERSION_NUM']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['AE_COUNTRY_ID']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['CD_DATE_AE_START']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_INT_REQ']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_DEATH']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_SOC']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['AE_COUNTRY_DESC']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['TXT_DATE_AE_START']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['DATE_AE_END']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_HLT']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['AE_NATIVE_LANG_ID']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_SOC_CODE']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_HLGT_CODE']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_SYN_CODE']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_PT_CODE']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_LLT_CODE']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_DISABLE']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_CONG_ANOM']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_PT']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_PRLNG_HSPTALIZED']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['REPORTED_AE']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['TXT_DATE_AE_END']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_CAUSED_HSPTALIZED']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_INTERNAL_ID']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_HLT_CODE']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_MEDICAL_CONFIRM']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['RANK_ID']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_HOSP']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_MDR_CODED_STATUS']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['AE_OUTCOME_ID']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['DATE_VERSION_END']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_OTHER_MED_IMP']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['DATE_VERSION_START']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['AE_REC_NUM']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['CD_DATE_AE_END']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_HLGT']},  
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['AE_OUTCOME_DESC']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['MDR_AE_LLT']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['AE_NATIVE_LANG_DESC']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_SERIOUS_THREAT']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['DATE_AE_START']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['FLAG_AE_MED_SERIOUS']},
    #         {obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['EVENT_RECEIPT_DATE']},{obj['C_AE_IDENTIFICATION']['ArrayElem'][i]['SERIOUS_OTHER_MED_IMP_TXT']})"""
            # print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    


    
def split_file(a,n):
    k,m = divmod(len(a),n)
    return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def load(file)->list:
    with open(file,'r', encoding="utf8") as fp:
        string = fp.read()
        o = json.loads(string)
        line_chunk = split_file(o,100)
        return line_chunk

