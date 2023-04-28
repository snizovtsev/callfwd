#include "Location.h"


// lerg=# select count(*) from zipdata;
// 32352

// lerg=# select count(*) from npanxxcitydata;
// zip_code | data...
//    47586 | ...
// count=173132

// lerg=# select * from npanxxziprel;
// id   | zip_code_id | npanxx_id
// 1898 |       49507 |    616298
// ...
// count=386403


// Query 1: npanxxcitydata[npa_area_code=201 and nxx_prefix=201]
// Response:
//    ...
//    zip_code=7302
// Query 2: zipdata[7302]
//    ...
// JSON:city=US,
//    [citydata].state=CA,
//    county=TELEPORT COMMUNICATIONS AMERIC
//    latitude=7142
//    longitude=JERSEYCITY
//    city_alias=CLEC
//    [citydata].lata=224
//    [zipdata].msa=xx         
//    [zipdata].msa_name=xxx
//    [zipdata].pmsa=xxx
