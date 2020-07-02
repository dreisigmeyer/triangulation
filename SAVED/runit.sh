#!/bin/bash

# initial work
cd ./inData
sort -T ./ -u prdn_metadata.csv > holder
mv holder prdn_metadata.csv
awk -F'|' -v OFS=',' '{print $1,$4}' iops.csv | sort -T ./ -u > iops_prdn_assg_seq.csv




# # # A models
# # ---- this is information for the B models
# awk -F',' -v OFS=',' '{ if ($5~/^[0-9]*$/) {print $8,$5 }}' a*_final.csv | sort -T ./ -u > Amodel_firmid_year.csv
# # ---- this is information for the C models
# awk -F',' -v OFS=',' '{print $2,$3,$5,$7,$8}' a*_final.csv | sort -T ./ -u > A_info_hold.csv
# ./Amodel_pik_year_firmid.pl
# sort -T ./ -u -o Amodel_pik_year_firmid.csv Amodel_pik_year_firmid.csv
# awk -F',' '{print $2}' a*_final.csv | sort -T ./ -u > Amodels_prdns
# rm A_info_hold.csv



# # B models
# sqlite3 prdn_db.db < create_Bmodels.sql
# sed -i 's/"//g' b1_models.csv
# sed -i 's/"//g' b2_models.csv
# mv b1_models.csv ../outData
# mv b2_models.csv ../outData
# rm Amodel_firmid_year.csv
# cd ../outData
# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' b1_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$16,$17,$12,$13,$4,$5,$14,$15,$2 }' trash.csv > holder_b1.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_b1.csv
# mv holder_b1.csv b1_models.csv
# rm trash.csv

# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' b2_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$16,$17,$12,$13,$4,$5,$14,$15,$2 }' trash.csv > holder_b2.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_b2.csv
# mv holder_b2.csv b2_models.csv
# rm trash.csv
# cd ../sql


# # C models
# rm prdn_db.db
# sqlite3 prdn_db.db < create_Cmodels.sql
# sed -i 's/"//g' c1_models.csv
# sed -i 's/"//g' c2_models.csv
# sed -i 's/"//g' c3_models.csv
# mv c1_models.csv ../outData
# mv c2_models.csv ../outData
# mv c3_models.csv ../outData
# rm Amodel_pik_year_firmid.csv
# cd ../outData
# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' c1_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$16,$17,$12,$13,$4,$5,$14,$15,$2 }' trash.csv > holder_c1.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_c1.csv
# mv holder_c1.csv c1_models.csv
# rm trash.csv

# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' c2_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$16,$17,$12,$13,$4,$5,$14,$15,$2 }' trash.csv > holder_c2.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_c2.csv
# mv holder_c2.csv c2_models.csv
# rm trash.csv

# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' c3_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$16,$17,$12,$13,$4,$5,$14,$15,$2 }' trash.csv > holder_c3.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_c3.csv
# mv holder_c3.csv c3_models.csv
# rm trash.csv
# cd ../sql


# # E models
# rm prdn_db.db
# sqlite3 prdn_db.db < create_Emodels.sql
# sed -i 's/"//g' e1_models.csv
# sed -i 's/"//g' e2_models.csv
# mv e1_models.csv ../outData
# mv e2_models.csv ../outData
# cd ../outData
# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' e1_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$16,$17,$12,$13,$4,$5,$14,$15,$2 }' trash.csv > holder_e1.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_e1.csv
# mv holder_e1.csv e1_models.csv
# rm trash.csv

# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' e2_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$16,$17,$12,$13,$4,$5,$14,$15,$2 }' trash.csv > holder_e2.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_e2.csv
# mv holder_e2.csv e2_models.csv
# rm trash.csv
# cd ../sql


# # D models
# rm prdn_db.db
# sqlite3 prdn_db.db < create_Dmodels.sql
# sed -i 's/"//g' d1_models.csv
# sed -i 's/"//g' d2_models.csv
# mv d1_models.csv ../outData
# mv d2_models.csv ../outData
# cd ../outData 
# sed -i -e "1d" d1_models.csv
# sed -i -e "1d" d2_models.csv
# awk -F',' -v OFS=',' '{ 
#     if ($10 != "") { 
#         print $0,1,0
#     } 
#     else if ($9 != "") { 
#         print $0,0,1
#     } 
#     else { 
#         print $0,0,0 
#     } }' d1_models.csv > trash.csv
# awk -F',' -v OFS=',' '{ print $1,$3,$6,$8,$7,$11,$10,$9,$17,$18,$12,$13,$4,$5,$14,$15,$2 }' trash.csv | sort -T ./ -u > holder_d1.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_d1.csv
# mv holder_d1.csv d1_models.csv
# rm trash.csv

# awk -F',' -v OFS=',' '{ 
#     if ($7 != "") { 
#         print $0,1,0 
#     } 
#     else if ($8 != "") { 
#         print $0,0,1 
#     } 
#     else { 
#         print $0,0,0 
#     } }' d2_models.csv > trash.csv
# #awk -F',' -v OFS=',' '{ print $1,$2,$3,$4,$5,$6,$7,$8,$17,$18,$9,$10,$11,$12,$13,$14,$15,$16 }' trash.csv > d2_hold.csv
# awk -F',' -v OFS=',' '{ print $1$2,$16 }' trash.csv > D2_prdnseq_name.csv
# mv D2_prdnseq_name.csv ../sql
# # sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count,name\n/' holder_d2.csv
# awk -F',' -v OFS=',' '{ print $1,$2,$3,$4,$5,$6,$7,$8,$17,$18,$9,$10,$11,$12,$13,$14,$15 }' trash.csv | sort -T ./ -u > holder_d2.csv
# sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_d2.csv
# mv holder_d2.csv d2_models.csv
# rm trash.csv
# cd ../sql

# #<<'COMMENTS'
# F models
# awk -F'|' -v OFS='|' '{ if ($4 != "") {print $1,$5,$3,$8,$9,$4,$10,$11,$6}}' ../inData/assigneeOutData/*.csv | sort -u -T ./ > prdn_seq_name.csv
# awk -F'|' -v OFS='|' '{ if ($4 != "" || $10 != "" || $11 != "") {print $1,$5,$3,$8,$9,$4,$10,$11,$6}}' ../inData/assigneeOutData/*.csv | 
#     grep -v "INDIVIDUALLY OWNED PATENT" |
#     sort -u -T ./ > prdn_seq_name.csv
# sed -i 's/|US|/||/g' prdn_seq_name.csv
# sed -i 's/|USX|/||/g' prdn_seq_name.csv
# sed -i 's/&/AND/g' prdn_seq_name.csv
# sed -i 's/+/AND/g' prdn_seq_name.csv
# sed -i 's/ \{1,\}/ /g' prdn_seq_name.csv
# # Remove non-alphanumeric characters and extra whitespace
# awk -F'|' -v OFS='|' '{ col6=$6;  gsub("[^A-Z0-9 ]","",col6); col7=$7; gsub("[^A-Z0-9 ]","",col7); col8=$8; gsub("[^A-Z0-9 ]","",col8); print $1,$2,$3,$4,$5,col6,col7,col8,$9;}' prdn_seq_name.csv > prdn_seq_stand_name.csv
# sed -i 's/|/,/g' prdn_seq_stand_name.csv
# rm prdn_seq_name.csv

# # To map D2 assignee name to USPTO and XML names
# awk -F',' -v OFS=',' '{print $1$2,$4}' ../inData/assignee_76_14.csv | sed '1d' | sort -T ./ -f -t',' -k1,1 > prdnseq_D2name.csv
# awk -F',' -v OFS=',' '{print $1$2,$3,$6,$7,$8}' prdn_seq_stand_name.csv | sort -T ./ -f -t',' -k1,1 > uspto_xml_names.csv
# #-- the most common name
# join -i -t',' -j1 -o1.2,2.2,2.3,2.4,2.5 prdnseq_D2name.csv uspto_xml_names.csv |
#     awk -F',' -v OFS=',' '{{print $1,$3,$2}; {print $1,$4,$2}; {print $1,$5,$2};}' |
#     sort -T ./ | uniq -c | sed -e 's/^ *//;s/ /,/' |
#     awk -F',' -v OFS=',' '{if ($2 != "" && $3 != "" && $4 != "") {print $2,$4,$1,$3}}' |
#     sort -T ./ -t',' -k1,1 -k2,2n -k3,3nr |
#     sort -T ./ -t',' -k1,1 -k2,2n -u |
#     awk -F',' -v OFS=',' '{print $1,$2,$4}' > D2_USPTO_XML_names_year.csv
# #-- other common names
# join -i -t',' -j1 -o1.2,2.2,2.3,2.4,2.5 prdnseq_D2name.csv uspto_xml_names.csv |
#     awk -F',' -v OFS=',' '{{print $1,$3,$2}; {print $1,$4,$2}; {print $1,$5,$2};}' |
#     sort -T ./ | uniq -c | sed -e 's/^ *//;s/ /,/' |
#     awk -F',' -v OFS=',' '{if ($2 != "" && $3 != "" && $4 != "") {print $2,$4,$1,$3}}' |
#     sort -T ./ -t',' -k1,1 -k2,2n -k3,3nr |
#     awk -F',' -v OFS=',' '{ if ($3 > 5){print $1,$2,$4}}' >> D2_USPTO_XML_names_year.csv
# sort -T ./ -u D2_USPTO_XML_names_year.csv > holder.csv
# mv holder.csv D2_USPTO_XML_names_year.csv
# rm prdnseq_D2name.csv uspto_xml_names.csv

# ./create_D2_name_maps.sh
rm prdn_db.db
sqlite3 prdn_db.db < create_Fmodels.sql
sed -i 's/"//g' f_models.csv
awk -F',' -v OFS=',' '{ 
    if ($7 != "") { 
        print $0,1,0 
    } 
    else if ($8 != "") { 
        print $0,0,1 
    } 
    else { 
        print $0,0,0 
    } }' f_models.csv > trash.csv
awk -F',' -v OFS=',' '{ print $1,$2,$3,$4,$5,$6,$7,$8,$16,$17,$9,$10,$11,$12,$13,$14,$15 }' trash.csv > holder_f.csv
sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' holder_f.csv
mv holder_f.csv f_models.csv
rm trash.csv 
mv f_models.csv ../outData


# Final crosswalk and cleanup
awk -F'|' -v OFS=',' '{
    if ($10 ~ /US/) {
        print $1,$6,"",$4,$3,$7,$9,$10,1,0,"","","","","","",""
    }
    else if ($9 != "") {
        print $1,$6,"",$4,$3,$7,$9,$10,1,0,"","","","","","",""
    }
    else if ($10 != "") {
        print $1,$6,"",$4,$3,$7,$9,$10,0,1,"","","","","","",""
    }
    else {
        print $1,$6,"",$4,$3,$7,$9,$10,0,0,"","","","","","",""
    }
}' ../inData/assigneeOutData/*.csv | sort -T ./ -u > full_frame.csv
sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assignee_flag,foreign_assignee_flag,us_inventor_flag,multiple_assignee_flag,br_yr,lehd_yr,model_type,unique_firm_id,count\n/' full_frame.csv

sqlite3 cw_db.db < create_crosswalk.sql
sed 's/"//g' crosswalk.csv > crosswalk.csv_sed
mv crosswalk.csv_sed crosswalk.csv
mv crosswalk.csv ../outData
sed 's/"//g' crosswalk_F.csv > crosswalk_F.csv_sed
mv crosswalk_F.csv_sed crosswalk_F.csv
mv crosswalk_F.csv ../outData
sed -i 's/"//g' standard_name_to_firmid.csv
mv standard_name_to_firmid.csv ../outData
rm D2_prdnseq_name.csv D2_name_maps.csv
rm prdn_db.db cw_db.db
rm Amodels_prdns
# rm prdn_eins.csv prdn_piks.csv
rm prdn_seq_stand_name.csv
# rm D2_USPTO_XML_names_year.csv
rm full_frame.csv
rm ../inData/iops_prdn_assg_seq.csv
cd ../
#COMMENTS
