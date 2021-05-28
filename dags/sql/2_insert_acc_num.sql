MERGE into dcm_qa d
    using ajrr_dicoms a on (a.acc_num = d.accession_number and d.customer='Total Joint')
    WHEN NOT MATCHED THEN
        INSERT (d.customer, d.accession_number) values ('Total Joint', a.acc_num)
