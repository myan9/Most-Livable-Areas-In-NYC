import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;
import java.util.Arrays;
import java.text.DecimalFormat;

public class dataAnalysisReducer extends Reducer<Text, Text, NullWritable, Text> {
    private String[] pp_by_zipcode = {
"10001,23332,23537,22767,21966,20579,21097,21102,154380",
"10002,78096,80736,79894,82191,80323,81335,81410,563985",
"10003,56767,57112,57068,57310,56614,55190,56024,396085",
"10004,3044,3221,3024,2807,2780,2604,3089,20569",
"10005,8710,8131,7570,6822,5724,4935,7135,49027",
"10006,3277,3110,2950,2507,2696,2722,3011,20273",
"10007,6740,6876,6748,6525,6188,5892,6988,45957",
"10009,59799,60865,61806,62810,62397,62335,61347,431359",
"10010,32854,31447,30708,30670,29396,28954,31834,215863",
"10011,50404,52349,52941,52167,51853,51064,50984,361762",
"10012,24536,24163,24846,26145,24703,24342,24090,172825",
"10013,28211,27415,26937,26065,26090,25942,27700,188360",
"10014,31464,30727,31835,30597,30893,31685,31959,219160",
"10016,50444,50641,50112,51196,52624,53754,54183,362954",
"10017,16533,16472,16278,16129,16137,15603,16575,113727",
"10018,9684,9168,8062,7021,6147,5290,5229,50601",
"10019,39505,38830,38752,38394,40351,40778,42870,279480",
"10020,0,0,0,0,0,0,0,0",
"10021,45261,43573,42142,40862,42646,41546,43631,299661",
"10022,30262,30607,29699,29618,29317,28847,31924,210274",
"10023,60307,60586,60762,61315,60447,60035,60998,424450",
"10024,58516,58391,59249,59164,58991,60239,59283,413833",
"10025,94717,96068,97373,97390,96449,95330,94600,671927",
"10026,38540,38310,38372,37871,36797,36026,34003,259919",
"10027,64120,64413,63057,62617,61018,58834,59707,433766",
"10028,48205,46883,46169,44295,45799,45345,45141,321837",
"10029,79411,79251,77857,77454,79031,78451,76003,547458",
"10030,30358,29209,29530,28472,29364,29186,26999,203118",
"10031,60778,59244,58912,59092,59708,58736,56438,412908",
"10032,63189,62685,59069,59374,61650,62798,57331,426096",
"10033,59353,59844,60520,58710,57073,58027,53926,407453",
"10034,43951,43405,42672,41510,41200,40925,38908,292571",
"10035,34187,33920,34760,33488,34709,35290,33969,240323",
"10036,25987,25799,24127,23132,22413,22371,24711,168540",
"10037,20112,18777,18041,17377,17099,17489,17416,126311",
"10038,22300,21464,20997,20082,20042,19530,20300,144715",
"10039,27471,26697,25952,26577,25372,25257,24527,181853",
"10040,45033,44378,45278,44825,43650,43753,41905,308822",
"10044,11731,11783,12050,12346,12311,11399,11661,83281",
"10065,30001,30237,31761,32797,32807,32193,32270,222066",
"10069,5231,5526,5523,5118,5359,5214,5199,37170",
"10075,23353,25756,24377,24849,24148,26082,26121,174686",
"10103,0,0,0,0,0,0,3,3",
"10110,0,0,1,2,3,4,0,10",
"10111,0,0,0,0,0,0,0,0",
"10112,0,0,0,0,0,0,0,0",
"10115,0,0,0,0,0,0,0,0",
"10119,0,0,0,0,0,0,92,92",
"10128,63006,61927,62447,60121,59443,58793,60453,426190",
"10152,0,0,0,0,0,0,0,0",
"10153,0,0,0,0,0,0,0,0",
"10154,0,0,0,0,0,0,0,0",
"10162,1253,1289,1252,1327,1590,1608,1685,10004",
"10165,0,0,0,0,0,0,2,2",
"10167,0,0,0,0,0,0,0,0",
"10168,0,0,0,0,0,0,0,0",
"10169,0,0,0,0,0,0,0,0",
"10170,0,0,0,0,0,0,2,2",
"10171,0,0,0,0,0,0,0,0",
"10172,0,0,0,0,0,0,0,0",
"10173,0,0,0,0,0,0,2,2",
"10174,0,0,0,0,0,0,0,0",
"10177,0,0,0,0,0,0,0,0",
"10199,0,0,0,0,0,0,9,9",
"10271,0,0,0,0,0,0,0,0",
"10278,0,0,0,0,0,0,0,0",
"10279,0,0,0,0,0,0,0,0",
"10280,9845,9552,8817,8685,7610,7741,7853,60103",
"10282,5635,5730,5903,6217,5507,5027,4783,38802",
"10301,39519,39209,39367,38885,38851,39367,39706,274904",
"10302,17758,18286,18043,16811,17090,17128,19088,124204",
"10303,24562,23933,24348,24537,24933,26274,26337,174924",
"10304,42054,42244,41864,43084,41845,40934,42193,294218",
"10305,41533,41795,41471,42006,41309,40085,41749,289948",
"10306,55031,54939,54888,55902,55692,55710,55909,388071",
"10307,15275,15098,15006,14829,14418,14363,14096,103085",
"10308,30347,29766,29805,28939,28782,29014,27357,204010",
"10309,33459,33185,33382,32646,31943,30706,32519,227840",
"10310,24250,24394,24719,25227,24454,24473,24962,172479",
"10311,0,0,0,0,0,0,0,0",
"10312,60951,62032,61105,60081,61136,60318,59304,424927",
"10314,88585,87600,87524,87276,87921,87662,85510,612078",
"10451,48289,48188,47818,47069,45931,43550,45713,326558",
"10452,75288,75559,75252,74931,74520,75150,75371,526071",
"10453,80516,80081,79002,79793,79143,79376,78309,556220",
"10454,38938,39012,38751,38089,37850,38523,37337,268500",
"10455,42779,41609,39951,38576,38114,37699,39665,278393",
"10456,92712,91868,91737,87723,87796,86121,86547,624504",
"10457,74010,73763,71015,70282,69411,69144,70496,498121",
"10458,81698,79974,76276,74859,74911,73436,79492,540646",
"10459,47972,47778,48269,47977,47000,45261,47308,331565",
"10460,60266,59512,58676,56084,56011,55462,57311,403322",
"10461,50255,51820,52435,51730,51599,51767,50502,360108",
"10462,78725,78504,76114,77369,75709,75407,75784,537612",
"10463,71657,71981,71238,70420,69371,68345,67970,490982",
"10464,4300,4723,4532,4248,3974,4255,4534,30566",
"10465,44950,45839,46360,44862,43897,42688,42230,310826",
"10466,71645,71138,69657,70609,71631,72997,67813,495490",
"10467,103045,101134,99251,98754,96319,93833,97060,689396",
"10468,75537,73637,72683,71822,73205,73738,76103,516725",
"10469,71664,72732,72241,71843,71580,71245,66631,497936",
"10470,14574,14868,14880,14850,15647,14952,15293,105064",
"10471,21601,21727,21935,22030,21792,22222,22922,154229",
"10472,68972,68654,69659,68898,67915,66041,66358,476497",
"10473,61818,59352,59948,58660,58403,57444,58519,414144",
"10474,12793,12686,12223,12519,12723,12466,12281,87691",
"10475,44749,43913,44582,43231,41625,41377,40931,300408",
"10501,1226,1226,1073,1217,1207,1206,1219,8374",
"10502,5513,5519,5396,5346,5361,5289,5487,37911",
"10503,137,168,172,158,263,222,108,1228",
"10504,7903,7910,7885,7960,8215,7907,7987,55767",
"10505,905,868,785,638,694,629,851,5370",
"10506,5963,5453,5303,5448,5158,5340,5790,38455",
"10507,6859,7004,7039,6801,7006,7287,6408,48404",
"10509,19419,19218,19605,20209,20148,19931,19507,138037",
"10510,10423,10428,10391,10166,10671,10336,9988,72403",
"10511,2263,2236,2349,2260,2354,2427,2246,16135",
"10512,25076,24845,25467,24778,24960,25079,25590,175795",
"10514,12688,12741,12915,13256,12604,12469,11946,88619",
"10516,5486,5474,5327,5223,5205,5214,5289,37218",
"10517,584,608,756,622,679,474,539,4262",
"10518,1478,1502,1505,1390,1561,1424,1268,10128",
"10519,137,207,181,166,126,109,316,1242",
"10520,12797,12996,13107,13113,13159,13267,12810,91249",
"10522,11058,11055,11001,10948,10889,10831,10875,76657",
"10523,9058,8752,8563,8029,7789,7687,7444,57322",
"10524,4268,4252,4402,4488,4482,4460,4421,30773",
"10526,1522,1662,1714,1571,1738,1979,1809,11995",
"10527,836,877,1156,1145,1221,1144,908,7287",
"10528,13066,12451,12720,12692,13209,13358,12280,89776",
"10530,12800,13011,12431,11891,11947,11796,12604,86480",
"10532,5019,4945,4906,5074,5071,5028,4931,34974",
"10533,7590,7487,7484,7662,7406,7419,7322,52370",
"10535,685,717,573,796,800,748,555,4874",
"10536,10775,11022,10725,10719,10840,10204,10739,75024",
"10537,1541,1842,1952,2242,2239,2463,2416,14695",
"10538,16718,16841,16803,16673,16699,16972,16597,117303",
"10541,26371,26850,26355,26643,26647,26328,26339,185533",
"10543,20643,20516,20250,20157,20040,19922,20135,141663",
"10545,131,129,135,126,114,111,141,887",
"10546,1433,1265,1149,1092,1275,1244,1277,8735",
"10547,7461,7045,6913,7421,7937,8171,7647,52595",
"10548,3398,3156,3214,3089,2910,3174,3487,22428",
"10549,16557,16353,16165,15798,15560,15475,16638,112546",
"10550,37679,37543,37421,36804,36938,36523,37144,260052",
"10552,19620,19607,20339,20815,20259,20116,19786,140542",
"10553,10606,10850,10032,9895,10141,10507,10170,72201",
"10560,4930,4810,4742,4786,4812,4819,4737,33636",
"10562,31745,31453,31453,31502,31239,31348,31796,220536",
"10566,23977,23928,23875,23702,23568,23392,23570,166012",
"10567,20652,20741,20197,20241,20083,19743,19929,141586",
"10570,12966,12889,12592,12735,12623,12501,12680,88986",
"10573,38958,38817,38656,38429,38237,38017,38352,269466",
"10576,4878,4803,4842,4948,4996,5261,5116,34844",
"10577,6508,6486,6269,5823,5391,4623,6552,41652",
"10578,843,714,589,534,665,773,681,4799",
"10579,9438,9192,9264,8969,9071,8741,8675,63350",
"10580,17270,17358,17322,17241,17342,17210,17208,120951",
"10583,40054,39996,39510,39601,39210,38204,38982,275557",
"10588,2748,2723,2518,2389,2090,1883,2282,16633",
"10589,9229,9102,9376,9158,8403,8348,8475,62091",
"10590,6937,7111,7026,6926,6632,6601,6767,48000",
"10591,23394,23185,23037,22890,22722,22548,22540,160316",
"10594,5076,5172,5201,5020,5114,5083,5117,35783",
"10595,8237,8369,8577,8407,8016,8071,8195,57872",
"10596,1727,1657,1629,1270,1152,799,1729,9963",
"10597,1154,1058,1128,1233,1138,954,968,7633",
"10598,29489,30030,29699,29234,28624,28717,28647,204440",
"10601,11519,11787,11524,10623,10113,10160,11376,77102",
"10603,17429,17223,17924,18142,17914,18138,17045,123815",
"10604,11153,11808,11397,11863,11607,11737,11250,80815",
"10605,17470,18118,17671,18337,18368,18778,18126,126868",
"10606,18312,17489,17293,16632,16410,15657,16499,118292",
"10607,6806,6307,6727,7098,6854,6903,6824,47519",
"10701,58841,59884,59628,61167,62396,62939,63393,428248",
"10703,21039,20856,20894,20390,20261,20367,20301,144108",
"10704,32125,31321,31198,30516,30432,29807,30165,215564",
"10705,41008,40392,39817,39924,38654,38142,38777,276714",
"10706,8776,8713,8734,8555,8549,8698,8679,60704",
"10707,10253,10653,10525,10479,10461,10330,10097,72798",
"10708,22632,22178,22497,21809,21464,21309,21225,153114",
"10709,9090,9357,9347,9303,9605,9586,9292,65580",
"10710,27602,27628,27388,26432,25811,25472,25120,185453",
"10801,42154,41577,41265,41093,40674,40120,40827,287710",
"10803,12560,12560,12523,12439,12470,12392,12435,87379",
"10804,14634,14795,14850,15096,15069,15019,14146,103609",
"10805,18503,18799,18898,18138,17758,17828,18414,128338",
"10901,23959,23235,23195,22875,23337,23007,23465,163073",
"10910,27,23,24,26,0,0,21,121",
"10911,0,0,0,0,0,0,2,2",
"10913,5626,5302,5657,5175,5277,5245,5532,37814",
"10914,455,366,421,224,310,287,414,2477",
"10915,14,104,168,158,158,216,119,937",
"10916,4265,4238,4374,4446,4468,4518,4540,30849",
"10917,2134,2030,1959,1808,1790,2134,1968,13823",
"10918,12264,12258,11700,12043,11766,11480,11647,83158",
"10919,1331,1405,1647,1346,1312,1149,1040,9230",
"10920,8877,8946,8682,8744,8648,8385,8554,60836",
"10921,3856,3861,4138,3926,4094,3926,4135,27936",
"10922,1568,1347,1406,1355,1165,998,1559,9398",
"10923,8796,8473,8705,8797,8736,8587,8732,60826",
"10924,13388,13255,13153,13141,12913,13091,13120,92061",
"10925,4061,4330,4300,4446,4302,4370,4539,30348",
"10926,3108,3370,3256,3354,2958,3009,3203,22258",
"10927,12120,12094,12060,11952,11843,11711,11910,83690",
"10928,4004,4066,4029,4033,4035,4009,4175,28351",
"10930,8784,9021,8938,9050,9029,8937,8958,62717",
"10931,887,926,908,950,988,1024,1023,6706",
"10932,106,100,33,106,142,71,60,618",
"10933,927,809,797,819,553,216,473,4594",
"10940,49194,48651,48512,47833,48209,48800,48418,339617",
"10941,13242,13301,13572,13959,14006,13585,13779,95444",
"10950,49712,49065,48838,47611,47742,46739,47226,336933",
"10952,41631,41891,41619,40193,39271,38285,38917,281807",
"10953,190,102,123,162,133,152,252,1114",
"10954,23226,23809,23678,23550,22908,23125,23045,163341",
"10956,31450,31039,31090,30909,31426,31335,31521,218770",
"10958,3121,2899,2929,3030,3187,3538,3291,21995",
"10960,15357,15253,15637,15476,15545,15261,15093,107622",
"10962,5581,5809,5677,6166,5995,5915,5950,41093",
"10963,4263,4361,4219,4260,3889,3669,4298,28959",
"10964,1367,1557,1711,1791,1598,1651,1472,11147",
"10965,15149,15320,14878,14794,14799,14710,14791,104441",
"10968,2249,2344,2301,2302,2194,2099,2353,15842",
"10969,1403,1717,1665,1674,1534,1522,1267,10782",
"10970,9773,9873,9716,9670,9164,9795,9993,67984",
"10973,2322,2603,2646,2789,2462,2286,2126,17234",
"10974,3208,3199,3166,3149,3141,3125,3152,22140",
"10975,291,297,328,238,305,455,281,2195",
"10976,2699,2264,2266,2134,2179,2087,2258,15887",
"10977,63319,62280,60650,60293,59133,57800,59048,422523",
"10979,283,301,201,174,137,192,234,1522",
"10980,13997,13860,13746,13440,13620,13330,13383,95376",
"10983,5674,5649,5727,5537,5522,5528,5532,39169",
"10984,3020,2880,3117,3317,3485,3282,2842,21943",
"10985,38,34,14,13,90,115,58,362",
"10986,1696,1836,1887,2103,1923,2013,1974,13432",
"10987,3280,3318,3279,3364,3320,3196,3395,23152",
"10988,711,869,947,819,983,902,896,6127",
"10989,10333,10033,10113,9868,9757,9934,9293,69331",
"10990,19678,19647,20029,20139,20526,20598,20631,141248",
"10992,9197,9048,9324,9839,9716,10460,9621,67205",
"10993,4996,5326,5069,4678,4559,4587,4769,33984",
"10994,7652,7490,6931,7206,6963,6861,7085,50188",
"10996,6612,6830,6892,7013,7286,7570,6756,48959",
"10998,2824,2736,2699,2759,2818,3082,3122,20040",
"11001,27650,27551,27156,27001,27104,26905,26883,190250",
"11003,45480,45406,44907,43739,44016,43910,41356,308814",
"11004,14722,13778,14045,14500,14178,14050,14016,99289",
"11005,1926,1947,2123,2055,1963,1797,1806,13617",
"11010,24539,24791,25054,24394,23800,23790,23821,170189",
"11020,6285,6378,6120,6370,6139,5975,5914,43181",
"11021,17856,17378,17478,17467,17449,17544,17729,122901",
"11023,9825,9953,9955,9870,9400,8954,9027,66984",
"11024,7790,7667,7789,7555,7784,7853,8002,54440",
"11030,17642,17505,17295,17372,17854,17914,17962,123544",
"11040,41381,41434,40799,40231,40005,40296,40782,284928",
"11042,549,549,542,544,541,534,520,3779",
"11050,30450,30845,30583,30666,30397,30197,30171,213309",
"11096,7701,8331,7884,8276,8091,8443,8344,57070",
"11101,26505,25880,25317,25537,24405,23798,25484,176926",
"11102,33750,35271,35027,34529,33742,33432,34133,239884",
"11103,38861,39231,38257,37745,36724,36508,38780,266106",
"11104,26102,26718,26555,25729,26321,26230,27232,184887",
"11105,37297,37194,36816,36190,37438,38103,36688,259726",
"11106,39409,38812,38633,36869,36958,36305,38875,265861",
"11109,4964,4702,4337,3524,3272,2916,3523,27238",
"11201,60061,58350,56488,54668,52242,51356,51128,384293",
"11203,75920,75887,77011,79572,80202,80337,76174,545103",
"11204,79332,80486,80349,80963,80341,78690,78134,558295",
"11205,44818,43224,43320,43002,40125,38997,40366,293852",
"11206,86486,84806,83180,81525,80086,79674,81677,577434",
"11207,93251,94657,93465,92491,92054,89676,93386,648980",
"11208,95715,93847,92262,93107,93998,93447,94469,656845",
"11209,67822,70803,72582,72623,72434,71704,68853,496821",
"11210,68435,67432,67042,65302,64041,64508,62008,458768",
"11211,99964,97772,94681,93271,90510,90522,90117,656837",
"11212,86469,88668,87751,84520,81267,80311,84500,593486",
"11213,65434,64603,62756,62059,61495,62231,63767,442345",
"11214,91728,91295,88297,83156,81428,78326,88630,602860",
"11215,69965,70818,70156,68891,67195,65477,63488,475990",
"11216,55816,53747,54440,53783,53662,53065,54316,378829",
"11217,38670,38511,38567,38787,38398,38992,35881,267806",
"11218,74831,75691,75132,74758,74571,76811,75220,527014",
"11219,96287,97670,98719,96971,95069,94083,92221,671020",
"11220,101006,102325,101715,103089,100476,99540,99598,707749",
"11221,84034,83372,83773,81321,80254,77041,78895,568690",
"11222,35695,35163,34580,34186,33718,33482,36934,243758",
"11223,79730,79385,76651,74606,74700,73208,78731,537011",
"11224,43444,43001,42698,42535,44047,45092,47621,308438",
"11225,60323,60180,59613,59182,60179,58987,56829,415293",
"11226,101787,99026,98299,98325,99285,99422,101572,697716",
"11228,46164,47190,46059,43929,43396,42585,41788,311111",
"11229,82914,81732,81351,81030,79268,77021,80018,563334",
"11230,88933,88589,87064,84219,84707,85757,86408,605677",
"11231,36302,35263,34605,34137,34393,35109,33336,243145",
"11232,29387,29216,29452,28824,28703,27925,28265,201772",
"11233,72159,71589,69637,68599,67661,66315,67053,483013",
"11234,95546,95912,95610,94259,95390,94891,87757,659365",
"11235,78237,76668,75622,74630,72447,71129,79132,527865",
"11236,100049,98276,97714,97217,97084,94643,93877,678860",
"11237,53678,54182,55258,55478,53031,52123,49896,373646",
"11238,53992,52999,51959,51895,52033,50840,49262,362980",
"11239,12468,12924,12943,12879,12850,12802,13393,90259",
"11351,0,0,0,0,0,0,0,0",
"11354,58551,56908,56433,56258,55543,53050,54878,391621",
"11355,82028,82790,83799,83221,83938,82505,85871,584152",
"11356,24405,24631,23800,23037,22947,21721,23438,163979",
"11357,39259,39737,40588,40872,40705,40516,39150,280827",
"11358,37461,37486,37634,38317,39143,38668,37546,266255",
"11359,0,0,0,0,0,0,0,0",
"11360,19309,19605,19916,19865,20688,20953,18884,139220",
"11361,29322,30207,30352,30597,29197,28926,28606,207207",
"11362,18198,18139,17897,17865,18220,17492,17823,125634",
"11363,7577,7662,7625,7354,7442,7052,6988,51700",
"11364,35068,34949,34902,34751,35106,35372,34555,244703",
"11365,44617,45041,43138,41924,41520,41283,42252,299775",
"11366,14042,13192,13146,13499,13593,14160,13532,95164",
"11367,42663,42667,41582,41022,39424,38835,41047,287240",
"11368,112982,112709,110385,107962,104486,103401,109931,761856",
"11369,37823,39001,39862,40761,40160,39677,38615,275899",
"11370,34445,35507,36271,37244,37755,38370,39688,259280",
"11371,0,0,0,0,0,0,0,0",
"11372,64109,64754,63202,63320,62857,63520,66636,448398",
"11373,96616,98554,100713,99159,97430,96474,100820,689766",
"11374,43158,43399,41792,42297,42484,42888,43600,299618",
"11375,71159,70723,69652,69757,69745,69266,68733,489035",
"11377,90883,90615,88939,86316,87284,87143,89830,621010",
"11378,33270,31756,30854,32268,32187,33672,34981,228988",
"11379,36082,35476,35725,35822,35680,34529,34821,248135",
"11385,103372,102209,100132,99379,99508,99295,98592,702487",
"11411,19300,19208,19177,18722,19357,20426,18556,134746",
"11412,37802,37832,37690,37176,36660,36932,34882,258974",
"11413,42699,42560,41689,40385,39291,39922,38912,285458",
"11414,28247,28700,28742,28094,28769,28480,26148,197180",
"11415,19308,19403,19020,19145,19464,18677,19341,134358",
"11416,27144,26625,25973,25950,24977,23852,24861,179382",
"11417,31742,32415,31556,31004,30325,29928,28967,215937",
"11418,39908,39308,36751,36828,36821,36005,36256,261877",
"11419,51642,49306,48370,49193,48119,48138,47211,341979",
"11420,50549,50820,50162,48449,48226,47243,44354,339803",
"11421,43341,43024,42745,42588,41872,40975,39127,293672",
"11422,33408,33245,32589,33280,33116,31739,30425,227802",
"11423,31823,31982,32053,31625,31425,30890,29987,219785",
"11424,0,0,0,0,0,0,0,0",
"11425,0,0,0,0,0,0,0,0",
"11426,19991,19842,19348,19575,18847,19165,17590,134358",
"11427,22941,23568,23460,24877,24601,24385,23593,167425",
"11428,19736,19744,20461,20307,19784,19764,19168,138964",
"11429,26739,26362,27506,28311,28571,29167,25105,191761",
"11430,179,186,171,140,101,53,184,1014",
"11432,64686,63533,62395,61687,58705,56900,60809,428715",
"11433,34703,34111,37927,32821,30851,30076,32687,233176",
"11434,66279,65892,65255,62809,61853,61183,59129,442400",
"11435,57627,55323,54523,53542,52288,52358,53687,379348",
"11436,20136,19425,18609,17636,17316,16956,17949,128027",
"11451,0,0,0,0,0,0,0,0",
"11501,19262,19241,19155,18991,18982,18981,19148,133760",
"11507,7330,7430,7224,6998,7215,7751,7406,51354",
"11509,2329,2174,2232,2022,2159,2250,2653,15819",
"11510,33734,33473,33430,32553,33152,32234,33048,231624",
"11514,4543,4913,5197,5273,5332,5395,4673,35326",
"11516,7724,7742,7513,7477,7486,7479,7556,52977",
"11518,10192,10278,10065,10243,10307,10610,10549,72244",
"11520,43697,43795,43632,43472,43246,43168,43341,304351",
"11530,28463,28663,28155,28019,27982,27835,27273,196390",
"11542,27676,27849,27691,27585,27530,27459,27633,193423",
"11545,12694,12426,12653,13005,13070,11962,12065,87875",
"11547,837,741,797,718,655,566,793,5107",
"11548,2858,2407,2328,2338,2347,2301,2780,17359",
"11549,2920,2864,2337,2429,2567,2740,2922,18779",
"11550,57679,57352,57224,56773,56347,56045,56435,397855",
"11552,24417,23672,22949,22565,22872,22975,23616,163066",
"11553,26160,26158,26506,26231,26430,26032,26034,183551",
"11554,37228,37836,37513,37687,38126,37390,38132,263912",
"11556,14,14,0,0,0,0,17,45",
"11557,6972,6590,7364,7538,7214,8047,7823,51548",
"11558,8323,8449,8646,8688,8456,8485,8370,59417",
"11559,8709,8403,8402,8188,8020,7931,8176,57829",
"11560,6354,6284,6334,6534,6628,6501,6464,45099",
"11561,37544,37458,37599,37894,37881,37370,37280,263026",
"11563,22551,22985,22984,23177,22953,22453,22666,159769",
"11565,8640,8677,8820,8752,8779,8909,8690,61267",
"11566,34232,33356,33791,33582,33980,34081,35321,238343",
"11568,4578,4356,4240,4000,3858,3802,4555,29389",
"11569,1229,1342,1284,1309,1225,1459,1398,9246",
"11570,27463,27819,27896,27235,26870,26917,26646,190846",
"11572,29295,28763,28901,28627,29540,29210,30574,204910",
"11575,16564,16386,16674,16200,16408,15677,16272,114181",
"11576,12663,12471,12325,12524,12060,12190,12366,86599",
"11577,12888,12444,12788,12541,12511,12230,12337,87739",
"11579,5128,5052,5336,5369,5265,5290,5055,36495",
"11580,43219,42765,41525,41129,40500,40441,40113,289692",
"11581,21406,21579,20956,21134,20553,20129,20844,146601",
"11590,47134,46938,45972,45656,44977,43599,45768,320044",
"11596,10683,10636,10638,10582,10517,10412,10480,73948",
"11598,13478,12874,12927,13167,12972,12896,13328,91642",
"11691,64946,63989,62577,61091,61442,59376,60035,433456",
"11692,19403,18955,17970,17674,16870,17070,18540,126482",
"11693,12021,11194,11338,11335,11415,11407,11916,80626",
"11694,21219,20773,21507,21725,21281,20792,20408,147705",
"11697,3928,4006,4078,4223,4344,4381,4079,29039"
    };

    private int findIndex(String arr[], int l, int r, int zipcode)
    {
        if (r >= l) {
            int mid = l + (r - l) / 2;
 
            if (Integer.parseInt(arr[mid].substring(0,5)) == zipcode)
                return mid;
 
            if (Integer.parseInt(arr[mid].substring(0,5)) > zipcode)
                return findIndex(arr, l, mid - 1, zipcode);
 
            return findIndex(arr, mid + 1, r, zipcode);
        }
 
        return -1;
    }

  @Override
  public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {  
      String curr_zip = key.toString();
      int population = 0;
      int index = findIndex(pp_by_zipcode, 0, pp_by_zipcode.length-1, Integer.parseInt(curr_zip));
      if (index != -1){
          String[] zip_total = pp_by_zipcode[index].split(",");
          population = Integer.parseInt(zip_total[zip_total.length - 1]);
      }else{
          return;
      }
      //String bor = "None";
      int count = 0;
      for (Text val: value) {
        //bor = val.toString();
        count += 1;
      }

      if (population != 0){
          double rate = (double)count / population;
          if (rate < 1.0){
              rate *= 100; 
              DecimalFormat df = new DecimalFormat("#.##");      
              rate = Double.valueOf(df.format(rate));
              context.write(NullWritable.get(), new Text(curr_zip + "|" + count));
              
          }
      }

  }
}

  