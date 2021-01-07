# make sure $PGPASSWORD env variable is set
psql $DATACUBE_DB_URL -v product_name=s2a_level1c_granule -f delete_odc_product.sql
psql $DATACUBE_DB_URL -v product_name=s2b_level1c_granule -f delete_odc_product.sql
psql $DATACUBE_DB_URL -v product_name=s2a_sen2cor_granule -f delete_odc_product.sql
psql $DATACUBE_DB_URL -v product_name=s2_l2a -f delete_odc_product.sql
