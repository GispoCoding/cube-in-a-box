# make sure $PGPASSWORD env variable is set
psql -U opendatacube -d opendatacube -h localhost -v  product_name=s2a_level1c_granule -f delete_odc_product.sql
psql -U opendatacube -d opendatacube -h localhost -v  product_name=s2b_level1c_granule -f delete_odc_product.sql
psql -U opendatacube -d opendatacube -h localhost -v  product_name=s2a_sen2cor_granule -f delete_odc_product.sql
psql -U opendatacube -d opendatacube -h localhost -v  product_name=s2_l2a -f delete_odc_product.sql

