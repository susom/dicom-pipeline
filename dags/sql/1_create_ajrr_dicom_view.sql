create or replace view ajrr_dicoms as
with preop as (select pm.mrn, note.pat_id, max("proceduredate") as date_of_surgery,
max(note.ordering_date) dicom_date from TJ_CLARITY_PROC_OR tj
join pat_map pm on pm.mrn = substr("import_key_proc",1,7)||substr("import_key_proc",9,1)
join shc_proc_note note on pm.shc_pat_id = note.pat_id
    and note.ordering_date between (to_date("proceduredate",'YYYY-MM-DD') -365) and (to_date("proceduredate",'YYYY-MM-DD') -1)
    and (upper(note.description) like 'XR %PELVIS%'
                                        or upper(note.description) like 'XR %KNEE%'
                                        or upper(note.description) like 'XR %LEG%'
                                        or upper(note.description) like 'XR %LOWER EX%'
                                        or upper(note.description) like 'XR %FEMUR%'
                                        or upper(note.description) like 'XR %HIP%')
group by pm.mrn, note.pat_id)
select preop.mrn, preop.date_of_surgery, note.ACC_NUM, note.ordering_date, note.description
from preop join shc_proc_note note on note.pat_id = preop.pat_id and
                                      note.ordering_date = preop.dicom_date
    and (upper(note.description) like 'XR %PELVIS%'
        or upper(note.description) like 'XR %KNEE%'
        or upper(note.description) like 'XR %LEG%'
        or upper(note.description) like 'XR %LOWER EX%'
        or upper(note.description) like 'XR %FEMUR%'
        or upper(note.description) like 'XR %HIP%')
union
select pm.mrn, "proceduredate", note.ACC_NUM, note.ordering_date, note.description
from TJ_CLARITY_PROC_OR tj
         join pat_map pm on pm.mrn = substr("import_key_proc",1,7)||substr("import_key_proc",9,1)
         join shc_proc_note note on note.pat_id = pm.shc_pat_id
    and note.ordering_date >= to_date("proceduredate",'YYYY-MM-DD')
    and (upper(note.description) like 'XR %PELVIS%'
        or upper(note.description) like 'XR %KNEE%'
        or upper(note.description) like 'XR %LEG%'
        or upper(note.description) like 'XR %LOWER EX%'
        or upper(note.description) like 'XR %FEMUR%'
        or upper(note.description) like 'XR %HIP%')
order by mrn, date_of_surgery desc, ordering_date asc
