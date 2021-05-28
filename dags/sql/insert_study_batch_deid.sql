insert into study_batch (study_batch_id, study_id, deid_id, destination_bucket, destination_prefix, status_id)
    select {{ params.batchid }},92,17,'irb10669-rit',
    '/batch-'|| {{ params.batchid }} ||'/dicom/${PatientID}-${AccessionNumber}' ||
     '.tgz:${StudyInstanceUID}/${SeriesInstanceUID}/${InstanceNumber}-${SOPInstanceUID}.dcm',
     1 from study_batch ON CONFLICT DO NOTHING
