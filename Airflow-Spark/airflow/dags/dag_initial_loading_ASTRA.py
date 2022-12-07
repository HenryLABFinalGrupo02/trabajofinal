import os
os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark.sparkContext.addPyFile(r'/opt/airflow/dags/transform_funcs.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/dag_initial_loading_ASTRA.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/casspark.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/tiny_functions.py')
import transform_funcs
import pyspark.pandas as ps
ps.set_option('compute.ops_on_diff_frames', True)
import pandas as pd
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from time import sleep
from functools import reduce
from tiny_functions import *

import os
os.environ["JAVA_HOME"] = "/opt/java"
os.environ["SPARK_HOME"] = "/opt/spark"
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
spark.conf.set("spark.sql.repl.eagerEval.enabled", True) # Property used to format output tables better
spark.sparkContext.addPyFile(r'/opt/airflow/dags/transform_funcs.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/dag_initial_loading_ASTRA.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/casspark.py')
spark.sparkContext.addPyFile(r'/opt/airflow/dags/tiny_functions.py')

import transform_funcs
import pyspark.pandas as ps
ps.set_option('compute.ops_on_diff_frames', True)
import pandas as pd
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from cassandra.cluster import Cluster
from time import sleep
from functools import reduce
from tiny_functions import *

more_reviews_users = ['qEEk0PuoH1dVa619t8fgpw','EBa-0-6AKoy6jziNexDJtg','ZGjgfSvjQK886kiTzLwfLQ','JYYYKt6TdVA4ng9lLcXt_g','pitYOVSsF8R1gWG1G0qxsA','8fPlzYWo0j_nQrJMeyF0Fw','TGgfqWnUaCf6DM7TLuNhDQ','ZaMB7VbOwaARjxdhXjODxA','QJ-ikvhuRcigSCAWJTrnqQ','1xS8Jj23zHx8axIVopG3wA','Zn8uhC3DjoKjjwBiuM8oQg','ftRgzVFzv6-TOCBXEOdWeQ','VMtyZjaEJB9nfmjr4xdVlw','cARxOd_5yKCgsCbUZ5ED4Q','BBTexIhkFIYnS0rd56vsKQ','bme7nh1NwRfF4U34TXFK2Q','HVl561l9Y3jwMKNKnoxOyg','LcqNuhqaYt5ekKzaRirmIg','_l0csyXqNIcb3vG-1qR8DQ','E1olskL76b9TrcClgD9oBw','xjQSpme1Z7Xw8XehRLpYuA','86Fgk1s0tAVioJtVnJeBHg','XeS-0ONS5uoR_OfgZQebrA','Q_ixtOGUKUbCQ_sXThQAwA','cWAKzsMt0iMjBNPuNJVgDQ','n2pX5Ae8xCUi2_WlwcTkXQ','RHpCcdTbnBkgpdTYvbbTcw','0WqEkKMu03irkMiEtsFxZg','0MeivhX0kZCfV3zMtHtk9Q','afX9bbJ01xYU4L3qKbACUA','v8aeoMqdrClqfZxNB4lWCA','A9Q-_QpJy1mHjlwwP2RwJg','KLS_AWthM9n6KLcBTCF_RQ','GU_4tHnCYE6aKGU2XH-zkA','INUDMj7EmrLlTh6qDNprAg','7ktyPHE-NGnWxarOqjIQiQ','1Y38tVMSPH8jwIGw_y_E3g','I-wqWdMhZ-cgT4YPxY20gA','tt2qIFKZumubxk_UwXhijA','uJM_dR32MnrPztZZaC1rSQ','5BL_mcIfYvU9L08syGCfEg','c8qFkI_VusWo0xZvkjfBWQ','XuadFePn8P6l5epbEFnfKw','CJ1qOThxVs_V6VTYRL4Eaw','v62bgesQ-CSGDS1rNqXZsw','wKmJrv-p7gI6kBvlXQbcFw','Ebp-eYVK8Jtq-toYAHdDHA','UM1ApB4_5GJcxfaYS8jD4Q','8RcEwGrFIgkt9WQ35E6SnQ','gaqPcK4kIN9_N7htRQhTtg','A3gMFX-PQNR0aomxaDGJkw','yYTykS0TpVvhAtm0tda1sg','ij0Mu58AKlflicmBhI-xMA','mp3GeUkEr1gmMV5ZThtBpA','kcnmjaBiItk7wqk9kLiupA','X8tAoBJPDqEhwwNuCQVa0w','dQeH3N509e8AzzoC9Xdy5w','wulZapNwEvd8JA63oxTVKw','vIr2vYWApBXiQv5qHFn6gQ','bdIueXy-Sc0DfqzXbm9AEg','nc0pxjUX3IUM2diqr5ME4g','SQtIhJLfyPCSQvoA-qy4uA','Z_9WJOW0l_4jROfkkQWT4g','3fsfZqiRJKjEAiLIBbxsWA','sZuL9iA687GMN6AY3XBCIQ','d82e3N5c24CDA9Svi_t0fg','cHVfkm_h_vKt-ZIWMYQcrg','kFQ0u8c6x76jTwmEPmKoWw','6FVPYJedLt3FTCQlamuscw','1NOyCzEPeO6D7_2zHztOUQ','qVEOVvc9gB29v7IAWsnA_g','mkEqJ61lS7gJUR6nqlbRkw','1o4iynPusBLozi0CpLGH5Q','mgkPEU9hRPb9WnDwx3Z5iQ','8Rb0XNjTenrhJgLR2ydStg','lbj1atSiHxGFrzN9WAeynw','FV3oC7l8P_JWKULr2tnLFg','z3nv7VqIQEm8I7MTDwM-Ww','1WE0E1ewe2ZZU0l3V5vNWQ','mzdYdUsMk1uzl7kYyF7HaQ','94ICh-yDZXOOGu-e33C6RA','mqCL4lkyQLiNXj_74-tMvQ','nFiYurEbPZBAOCURMrBgag','jYknm57qpeZzTueiVIYTOA','K4myo2_Z_2KKdORpfdKoTQ','a6jqAsvBRT2mCkLhNjEY2g','vKGevla8lDHp8yxq4Poyfg','Hs4HViBxhjOkUJlthNKNWQ','uinzvCuEMwTRPcmgDtlAsg','KGIBBWFmyxnsc_-Ni0knmA','yrAX4ikJePO6eSN6_g6buw','PCISmI0atmUGCauThxGkPA','HYlb7vXGuiTvR5eiwSLBoA','leCO_9EFERggbuUoLYZ4gg','EmPcdONs6821YhkRQ8s87A','3zeNqvWimKiA9gOSbx2e6Q','hmVE7u0Nt45j5sZCH9j_YQ','uPdFJ22SxG74yhsatFVvrw','gcFwdhZfdhIacWQYbaLcBg','VwGzHDCtP9aZgXrUmgAajw','H88uWBzHemj6ldYJKU9-PQ','JclBkdNeLD0hpv9AR_HaeA','IwknZ7P0VWHSxOGsf4uMxw','JQe6xrPnYWZ3rWDbgwWZsA','DWASYCVz1yN6ytAFzfgEMQ','VZWt5nqlsjMntB2Ga97H5A','D-hz14YhfL_5d-BhcD02kw','HTFyT2aSIUUNYhw6fR99og','--4AjktZiHowEIBCMd4CZA','M9L3vxwilf57pxIk7xlQkA','BlU_lOa_o4FOG5wlvxRWpA','nXF3s0JyuSkxc01GaQLvOA','yU5TuZiKiG7VTgNGlffsYg','kaa6i7IHMyYIom7DIul9EQ','3A_Al7CYUQoLxK4CtnyabQ','5u5tto7PC3a7FQhIWEXflw','zq2nd0H1dVzi3bta4Zepuw','gbt3Ft7YmAmaxc8b27QM6Q','kQIp9HBSCjVfoGGV2wPLQg','MpBDUJXrry00iPnX5hbQGA','k6GurG0wmHVyiZGzmxs8eQ','KfPVXIzWw34sp9l5pfBE5Q','DQx7XI6ZxubfMndzFY22zQ','Vcbxj8JlIXkWgdvroc1qAg','6-DBwoNPw5zenYYSg6RrhA','WKJoektrmcMssSoQSI59ow','V0Kg9fkgfoCNvYpNJcv0Ng','hPuQIudnLHBJaJL7swpB8A','VZSLr8PCzqeh31HU8D9hXA','xKfuwsPWEaJwQiGaTYHelQ','Fq5CvUC1AzK8GSmpyn0UeQ','Khecid8LZRq9n6xe4qjgLA','h6uPago5wJSV4nPBO71iRg','IDKqeErl2l-huYugUzea6A','RGOJ68eoobRsYW4LgQtBcA','DGJRD862ewpN3Ux2pkZkHg','3PunMPL-ne29d2Oeq2PpXw','y5wLD2bvZ91fZnNid8Yh5g','ApOMEpkxD9yJTaaTxMfttA','v5PBVjPxj_W4-x89Lbc-TA','GQr0P3dgOZGSM3PYPoEtOg','HPlUnYMqvb5sGmX_paxCNQ','HXKZYi1d24WdafgmKhVUow','698q2zdsA2i9fYN1YhX1Xw','2anfbsmeoqeMYCiNwUow9Q','U8WHL_eN_XYntCINBrjThg','QsByY2gW90viW7wiqWeEcg','gyAfijhxmJzvwxPD9y6RHg','oh1jk_g-LqpKybskPYoP0Q','DtoCCg1vpf_Mj5Qk63zk3w','_w3Zqx8DiAEw6hT980Upvw','vxw6ilzQ9gKIfxLP3GvVrQ','eefUAeYwjVLTDu1iRxn7Iw','4gBvBdTybB8yydxh1nhOeQ','dhQOA8-FdpA6D0hNhXEYIw','Ytxn9vgPaA6X_1dKO_WNSw','SUV5QErmlMLhwsIkdUzK4w','TS_3tnBpHnlBYAoe978Xww','RbT0tdUrDiazlTGaORWGhQ','RsI5Sjjv1-EYue9yVN6WOw','hO0vrPqf4Ma2foebnR44Iw','bSRQp1PBDzCKyvepEQSYxQ','GrCKTYu9qPiUlowByh3VAA','2_1YJigBXrIll1fjVDS30w','eILPNE5BR9yaS1R3jlks9w','8uX0U8FyL9-HwNa64m1FMA','IjaFggQWriqk2rWv6RpPvg','IzkWFIoq-8k4wX_hV71Qbw','3iRkz8HR7qorxpx9bifywQ','QIogFooFd7UTIEx5iIU5kw','b7Ab6SBEvffhOX836lxATA','AjdePuDfMJTHphuJCQBdSQ','XBNtqhc72X57VK8GLNbsGg','_rWCX5E8R_3Ufv9JetOtHw','H6sRbrU6hePN89irFWBKew','tekHDsd0fskYG3tqu4sHQw','AxLCLK3Hxv-E7EntE3Y5gw','MxlkrZRxrSdJsPRdg1521A','ua3zAwmTMGvWdBR_rrjuzA','7BtVy5wVDth3-lwFbUgDXA','1GnIzms3F3MJJK2kyIwuHQ','y9dE4Q3D2Pu7dvVGdd-9Yw','UrKgCQvSTSUay1GNCkjnhA','HSQWc-Rgc_OpUTdKmSLjkQ','5UJ1ToXJ1Q4hpINbHcOJSQ','TA-wRnDB3Xv7_pSajJIUBg','jDWCDWUujfFpChAX8rgWYg','mPE3PKQ947F92s-4XvhQag','6UEJvMhHYIDzXI51lHPiIw','Usrg_bh_e7vJVwd0EMvkOw','a4WIm73rjfz1Z9_QGCGmaw','h4B9ZDALqcGev5XPyZ-Pow','iBQKwkuDvAdTM5gLWHgZwg','W1b5tQZzFN5KinOwypQDig','aDWWdGPZTyFkMGNyEwaVZg','bdfx_mqNeiYm-UKAkgRnRg','TBRQIFGGWjR7Z5Ki5_PfxQ','1-SMe3meQDCUyOypIS28PQ','dod4blQ8cwm3fdE00YFC0A','QBUFj_B6BH3QKzZpYHJz5w','zqCiUyWGnh633-2vLmuemw','88Ij88GMC7tcIegOicRdYA','4_IJKLhoyof73pvRal4Exw','vupwwaLgP0sVb6Qrc3Arvg','2kH_cKV0s_uZEIlCoHnzvw','W5fchLNpH6GmiVxx24zXgg','Sa9Jbde7d2kuzhVEAMRWEg','CfhitdMbQ88r9nlalyEb4w','Mv779di54QwcI43AO2mPLQ','xLcwDhFKONBP2F16lsaB_w','29UB_wrnUIdsxV2ZmrlZSg','zF_unBFpdWMY6el-B7wO8A','K1Cfhs2mESLD1nUWWfkFwQ','0qaUsn4TGgJYfbSxg_Fp2A','aAvl82YfsPmnNOMSMD8jBQ','Xjfc1owyGoFcM94kRxOCJg','fvgyz-tffn87IqaVkkN_DQ','NYxNe3Q5ROXBe7Fatst52g','GdcVn9hdpRa_rlYiEFaN3w','8i0Do65MQwRG4HAzO_detw','5i-nwtUBzji5An5fr4cpgA','m1q4gB6wdawoQXjPbs29mg','TDCb1p7KCiOd71tkY2rJ5g','4HzAp_Ck-hNIK7f4hEyKXA','BeS9uhpmc6Icap_Eso9Lqw','G3UtIVC0Sl8cbiA0_cPyMg','MvjdQ7T9YiBC9bL9fMaN3Q','ywH87wgOyI6ttRh0EgEeHg','CNmCBh1WQs-5a8RwIq0sMg','91z9RokpfMdwbxkdAA3-nw','aVwveJmgYeeN-ZnDX_shBw','IAReqg4gfz2OHz84p-qQ2w','ey88ywOdRVGEMmd6YyJNIA','tCti1lZLnkt5TXdOEdXqoA','7QTh-fkw9Nr2lO10-PV8yw','GQgH--hxKJisGmfsKj0xDA','zfvjfJ3RJR80A5D81emi4Q','NVxCOYSTlcRpnxaGop9N_g','Wjlo5YcDe2Lc7I4WNqooDw','7SgM5DrVSfkaqCpyeKjT0g','GlprrSVlHYCjhQtK1_LB6Q','337n8uBnr1pEP69JzVWDCQ','ft-zQNrmSXpG2heoTorC2w','UgYUeboBoETAgxom9iRdJQ','E_SoAPv9OT-eEzDZPoz2Tg','fQ2H73hk2r8vTeh-z7EhbQ','q8lvniKIxLt-d9zQ4yeEVg','HHkcBGeuKN55Ka5aULarmw','YqZniLami3yOXF3CSVxaFQ','7ziWZULyiZv2TesYNMFf4g','zDj-OYDs0dHjg2aXcVxD9A','IE4xKfypWuM3eaaq5q9IxA','xKsTgilmON3UrhEpM5Zl2Q','5Q5aeMq_dgomcVaSuvN5Lg','3R43ydfitOG0B4e_vuQyfA','6wRHe58b9X1mP7C86agykw','h59iX3YOfP1laPF000BV0Q','UnfeDunSlKcxvy2LPlzYBQ','AVNQyhynAB5z6iuN7P3Jfg','4LbxeQIyej3mr6HYYTsd7Q','WQ-_hGAsrAeR9QrZ4HH2lw','mDFeDwPLtG2TERrsR9kudw','uR4wrcLxQ7VUGBjM00iHNQ','5idrssvX83bcWhz11o4e1A','xW2A0MciHB0pLB4RHTi0nw','Q6vpjOTgGziXBkyURnUOqQ','wiIfZ8T2JdsRb8OKmdQj0Q','S7bjj-L07JuRr-tpX1UZLw','Nx-pMoHLNgB9ZVE3vBP05w','4reeeVHHWyZOp_Ejq5n40g','mSyINFt6TtXX9OHbwJfN5Q','yBkMMjXlQCEl7hiIGhQYgw','8Egic8Gg5qH0zC0mzB8BQA','rPzhe3wPt_68L3wrEhFg7g','d7qDp1KKg7GOLUMJLrlUsw','SjiTI7mX1UKdJbPLYeGI_g','3ywIG2FLBr9JNp6dnc8u3g','dEfQnSNy5US5FT1o5CMRbg','DaqsvhFFWRPMdLkriySHbg','acgJkn8BuKbBiKK956mWZw','LqC5hNXcFVY-jcbyxi-tuQ','GltpBXG2dCiMvi-bxvJPHA','1PhM0nRN6WN6bZadHN5cWQ','srou4DkAZLbVsgAPGqzN-g','ppYEK9zY7Vlhdf855ehlHA','C7hvShEj1G--Rc0exx6seQ','PGkMTm3hiermfR6TE498bw','LQ2CDCzIkXrUMXnh-W9QSg','RUQkywD4HQv2hvfv69Qq5g','bdalWswHTzYC1RDBYLehgw','TMMXLS-gEUQYDirHhL-9Og','yf7Wj4KMHh4Jv8Jwir8Kgg','QQwcp_Fmv7I3PLDAgUP1Mg','Oo_lq_YEKYfTSIO-1fnQvg','M6oZKl57_5bz0CeCCADdBA','-LpL5lWFDRytw5PADY1p5Q','P26U3x0gM5cG1s4gvWM82w','1Twew_ar_EPpb-NHSfcrog','azJSQWKdhUCsFELul6sVnQ','d5n7YWztGkm36c2LBQ8Y_w','oZ7y2hZHfnaoucaZC9Hp4g','NfjVkSRNironQBK0kHNoJw','DUNV27mVp5ATW6-J9LOJeA','8NjgVWDhLFNOTy19wJBoig','nfMgsrqADr_94KitluyCCQ','Du6bF8BUoJsIAuC0J3KLIA','xRJyMR1_QPuZuUhUuVaooQ','sMcCc4hcmnr_yolsz80gjA','mkQz8aihI1V19sYH8JQy-A','xSlBsTTPtPvwlclBIvAjYw','AO7thy9_PAXAT3rze_iaXw','c1XHqYtBl5qyeAspzPiCFg','sxGxgtlI6ipSimdUlSC0Fw','HmmBrh2_jdngGxjwzzzmdw','NiJmVx6JPcAUoE3d4j9aVg','ljqXU2O8o1Ofuu_CNRnXxA','RuYYtPAdhVjQ6u6GqmgZHw','6eErC2_WqGtCMO1tgqT1CA','CaSCl3j_tXFPhF6qzQciBQ','npaqVmBLp_bRk4DcraZYww','b9Q74D4XvFjJHKvLBLSaEA','1VJHfBEGw5xaEKcVSr38kQ','G35tigeQvOPqRVMzJVY9IQ','d-4Diz5jY05LPUE0mCBXlA','KJIzmEQ6peS6yc4RjlKS4Q','MVfbSEGqvhDWdmAfUeG38g','tNq35k92WAkSLSqeof03hg','aYQjCpAWtZMFS_Ce2ogvzA','CcXSCGzlhN8Of6xwafPkIQ','i2pV48tnyrf779rPkjnilA','H32QTRV87Q6CGTVUm1Z4QA','M7C_aSxzZyk93KNUmPUpYQ','k24QIVcmhhOnv9MCY3dELA','zxeXnjqmlrAspfk17LSZCg','mpnA1B30uMYGeuvKbJJgEw','6hNft_Qgu7T3b1X9B1-K5A','n1dDZfXcqw2A3iNwU5xlzg','WAzX3AaM6PQfzFJHaUF6xQ','2gWG2KZEhyIXXy5SOiUOZg','5BpTDdcXblUeG2dV-xciWQ','JRsGihBLWYVeeWRLP3in9A','qde7cw7YIJutTmRiFTEmfg','gqDbe1YwS4EhuCCNSNhpxg','mprkVxwLfSj6gW70USN7xA','41TYTuV87wmkLe-nrbwS3w','_1pVMv3nA38y5V9Cz8Zu_Q','QxVm4V55Dtdfv9Zfwm4daw','3dWsbFVG1c2iPzBM-WOQNA','39eSgXJRZwdWHs0IDAtzhQ','ovybrUwHdWRbFlKSM5H2Yw','kE8t6sP0yc4d47caMOyifQ','9IW9u_N2FfV-y5sUmLpYKQ','9ZiofeADDh_whZD8I9KDFw','g794-dhjC61kWSn1l3PoQA','-F6PdlUlQMGrsX-qug_f4g','KkmADj2xPhJL6DbZRVQcLQ','7xvUccpmp1fXWLAkZaXkDQ','G6ZnatT96yzdcX81PZyT3g','VEWzJnleYqTOVglmDALEyA','zFVBIOECiSpDb-e1WZOFVw','vzNYQaFyBnIU_bBkJsUJQA','Ng7RCiL-EjwAZo2jZoCwkA','5dXjCqtB1P4U3u6y8eEaZw','__1iEm-E3tvKuToIOCb76Q','IhM58htabC6OfgOdDyEW-Q','aD-xli-bVq6ABWuC_OqWIQ','riWDgbauId8TK7YFVgMNJw','uh3QEknKPHRG1wVABRYqWQ','3dcFnCZzBZrcKDeIYgiScw','J0Q7h3aSp4y1ElwA_JRAJQ','jlG7lVsF5LY2CxQUCGeQKg','0lSEYfAUQCqyBIaEQQUbbA','1OY-70r-JQaLbRbRBm04lA','9OJdZbS-8DiEtzqicUYQ8g','5dWx3ZbicVHtW7yGndsRoQ','4a7y9WOKYJmx5Hk_aUhmgg','i0FuzL8Aa5wV2O-n59_pEA','9woNV9FkJMX4MHygee84vA','BacuN4z2jKKPyBYV_wnAcA','xZ5TBpdWkHH15WTDgAqYFw','jheopFKRPEDAR_ur5HGTug','s16-BUo-orUsELvMu5ocKg','Wrdbwj4kmvlpptA0-ErOdA','yvQty5DgFfXlzZjnXteVbA','sruMjGMA3KbtDlMNvhwRLA','-G-s0z4MVaIjHIRAWp1myw','h8o3NmCcg3BAIHsBsm32Sw','xU5bD7aUKb9-LulOeBxCmQ','LrTOhEVai01haBAIMUY7mw','LimLRJP6n8pDPAT8knRhyw','0RJcckForPWsnVNMn_orYA','L8nFSWzvcsvLKKEekT5GuA','f2MiAt7rHnBq7LPL_9iqvA','zn_biH8_d_2j9wVgXj5Fsg','00hnvyIyIheNoyzhcOLZUA','A03MjVYocHO-6xOuk59ssw','WmfhKlnkeDYXwsY55xSO0g','9jV3AfGkcwPZNB1zR3jqbQ','Jd-ccxgw6bPWZMpiNNGijQ','XPzlQJDXbIPn71Hsgykugw','_l01jFciQKtGR1BuvrMG-g','ZnzpvxOa7eiLESrebeufJQ','q1MxLnDtvth41eaEogCeHw','Q7uJYSicQdI_RZbG8kjnSw','UYU71DRx-UDAee-eFOFfWw','R0w9KhptxXufhSt8dMNqnA','c1U0sTwSFFxShqB3leLoWA','0TL2UvlfvIe6g53JhjpiEQ','RNI2SJkPGW9L4Wm2YSxcMw','_XrNNcEbNnut2Q4Py-LfKg','60w9x_k88BtoVjwGLFn6nw','Lk6yr0Qff2ND7gzU075PRQ','ziIKYrsEnWOIogJhYJnVng','b9HV74ZB2neuy51hWstUGQ','dbELeRFW0v_bXybQNfb-_g','z3pethjEj75TLrX66FhUFQ','PCDorKmP5__w3BkyzjA2Xg','9UWf_0ohtl3_yqHdUjUe5A','wRfQaWiGKiR_htiSEkauSw','8z3dCuY4wurac7jhj7Oxew','ZOsDHReu1-GZuGgm9CeJQA','Rj88JTogoHMKg0wy9t3vhQ','YfvysNidwMsQtw56XAnGdQ','h6a1CAlAN-vm-JgwUoWeSg','adh20lqqWiv_cNhDfkPkNw','W9UO0ghofZM4WrFws44nGQ','v0JgXAAo2a1v5X2rSIpIPA','fkE8bjLL-fxYj-1_rlq28w','k6zYbMle3LI9jhMuY4Tc4w','q34MA5yPVVEkIV2mldl0jw','EEJZi7OjIY21ec-KXDa15Q','ySQP6km_KmfSQ48h1FxqLw','26F2byB18-dq7319tJHXrA','MksXytmVtf3oELjYMBlJ0A']
more_reviews = ['GBTPC53ZrG1ZBY3DT8Mbcw',
 'pSmOH4a3HNNpYM82J5ycLA',
 'PY9GRfzr4nTZeINf346QOw',
 '8uF-bhJFgT4Tn6DTb27viA',
 'EtKSTHV5Qx_Q7Aur9o4kQQ',
 'vN6v8m4DO45Z4pp8yxxF_w',
 'W4ZEKkva9HpAdZG88juwyQ',
 'UCMSWPqzXjd7QHq7v8PJjQ',
 'SZU9c8V2GuREDN5KgyHFJw',
 'M0r9lUn2gLFYgIwIfG8-bQ']

def connect_to_astra():
    print('ESTABLISHING CONNECTION TO CASSANDRA')
    cluster = Cluster(contact_points=['cassandra'],port=9042)
    session = cluster.connect()
    return session

def lower_col_names(cols):
    new_names = {}
    for x in cols:
        new_names[x] = x.lower()
    return new_names    

def load_top_tips(df):
    ##### UPLOADS SMALL DATASET TOP TIPS TO CASSANDRA

    #### MAKES PANDAS TRANSFORMATION TO GET TOP TIPS
    top_tips = pd.DataFrame(df['business_id'].to_pandas().value_counts())
    #top_tips2 = top_tips2[top_tips2['business_id'].isin(more_reviews)]
    top_tips.rename(columns={'business_id': 'number_tips'}, inplace=True)
    top_tips['business_id'] = top_tips.index
    top_tips.reset_index(drop=True, inplace=True)

    top_tips.rename(columns=lower_col_names(top_tips.columns), inplace=True)
    top_tips2 = ps.from_pandas(top_tips)

    #### CONNECT TO CASSANDRA
    print('ESTABLISHING CONNECTION TO CASSANDRA FOR TOP TIPS')
    session = connect_to_astra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.top_tips_full;")

    print('CREATING TABLE FOR TOP TIPS')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.top_tips_full(business_id text, number_tips int, PRIMARY KEY((business_id)))
    """)
    sleep(5)
    #### UPLOAD DATAFRAME TO CASSANDRA
    print('UPLOADING DATAFRAME TO CASSANDRA FOR TOP TIPS')
    top_tips2.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="top_tips_full", keyspace="yelp")\
    .save()
    print('DONE')
    print('DONE FOR TOP TIPS')


def load_user_metrics(): 
    
    """
    The function takes a dataframe of users and returns a dataframe with the influencer score for each
    user
    :param user: the user dataframe
    :return: A dataframe with the columns: n_interactions_received, n_interactions_send, fans,
    friends_number, Score_influencer, Influencer, user_id
    """
    print('READING USER FILE')
    df = ps.read_json(r'/opt/data/initial_load/user.json')
    
    #user = df[df['user_id'].isin(more_reviews_users)]
    df = 0
    user = user.to_pandas()
    print('DROPPING DUPLICATED ROWS')
    user = user.drop_duplicates()

    print('NORMALIZING DATES')
    user['yelping_since'] = user['yelping_since'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')
    print('GENERATING INTERACTIONS RECIEVED COLUMN')
    user_df = user.copy()
    user_df['n_ints_rec'] = user_df[[ 'compliment_hot',
    'compliment_more', 'compliment_profile', 'compliment_cute',
    'compliment_list', 'compliment_note', 'compliment_plain',
    'compliment_cool', 'compliment_funny', 'compliment_writer',
    'compliment_photos']].sum(axis=1)
    print('GENERATING INTERACTIONS SEND COLUMN')
    user_df['n_interactions_send'] = user_df['useful'] + user_df['funny'] + user_df['cool']
    print('GENERATING FRIENDS NUM COLUMN')
    user_df['friends_number'] = user_df.friends.apply(get_len)
    print('GENERATING INFLUENCER COLUMN')
    user_df['Influencer'] = user_df['n_ints_rec'] / (1 + user_df['friends_number'] + user_df['fans'])
    user_df['Influencer'].fillna(0, inplace = True)
    print('GENERATING INFLUENCER SCORE COLUMN')
    user_df['Influencer_Score'] = 1 - (1 / (1 + user_df['Influencer']))
    print('GENERATING INFLUENCER 2 COLUMN')
    user_df['Influencer_2'] = user_df['n_ints_rec'] / (1 + user_df['fans'])
    user_df['Influencer_2'].fillna(0, inplace = True)
    print('GENERATING INFLUENCER SCORE 2 COLUMN')
    user_df['Influencer_Score_2'] = 1 - (1 / (1 + user_df['Influencer_2']))
    print('GENERATING NEW DF')
    user_df = user_df[['user_id', 'n_ints_rec', 'n_interactions_send', 'fans', 'friends_number',
    'Influencer', 'Influencer_Score', 'Influencer_2', 'Influencer_Score_2']]
    print('TRANSFORMATIONS FOR USER METRICS DONE')
    user_df.rename(columns=lower_col_names(user_df.columns), inplace=True)
    print('UPLOADING CONNECTION TO ASTRA')
    session = connect_to_astra()
    
    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.user_metrics_full;")
    
    print('CREATING TABLE FOR USER METRICS')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.user_metrics_full(
        user_id text, 
        n_ints_rec int,
        n_interactions_send int,
        fans int,
        friends_number int,
        Influencer float,
        Influencer_Score float,
        Influencer_2 float,
        Influencer_Score_2 float,
        PRIMARY KEY(user_id));
    """) #friends list <text>,
    
    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA FOR USER METRICS')
    user_df = ps.DataFrame(user_df)
    user_df.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="user_metrics_full", keyspace="yelp")\
    .save()
    print('DONE')



###############################

def load_tips():
    #### READS FILE AND MAKES TRANSFORMATION
    print('READING TIPS FILE')
    tip = ps.read_json(r'/opt/data/initial_load/tip.json')
    #tip = tip[tip['business_id'].isin(more_reviews)]

    #### TRANSFORMATIONS
    print('DROPPING DUPLICATED ROWS')
    tip = tip.drop_duplicates()
    print('CLEANING STRINGS')
    tip = tip[tip['text'].apply(transform_funcs.drop_bad_str) != 'REMOVE_THIS_ROW']
    print('NORMALIZING DATES')
    tip['date'] = tip['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')

    #### UPLOAD A SMALL SUBSET TO CASSANDRA TOP TIPS
    print('UPLOADING SMALL DATABASE WITH TIP COUNT BY BUSINESS')
    load_top_tips(tip)

    tip.rename(columns=lower_col_names(tip.columns), inplace=True)

    #### CONNECT TO CASSANDRA


    session = connect_to_astra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.tip_full;")

    #### CREATE KEYSPACE AND TABLE
    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.tip_full(business_id text, date text, user_id text, compliment_count int, text text,PRIMARY KEY((business_id,date,user_id)))
    """)

    sleep(5)
    #### UPLOAD DATAFRAME TO CASSANDRA
    print('UPLOADING DATAFRAME TO CASSANDRA')
    tip.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="tip_full", keyspace="yelp")\
    .save()
    print('DONE')


def load_checkin():
    #### READS FILE AND MAKES TRANSFORMATION
    print('READING TIPS FILE')
    checkin = ps.read_json(r'/opt/data/initial_load/checkin.json')
    #checkin = checkin[checkin['business_id'].isin(more_reviews)]

    #### TRANSFORMATIONS
    print('DROPPING DUPLICATED ROWS')
    checkin = checkin.drop_duplicates()

    print("CALCULATING TOTAL CHECKINS")
    checkin['total'] = checkin['date'].apply(lambda x: get_len(x))

    print('NORMALIZING DATES')
    checkin['date'] = checkin['date'].apply(transform_funcs.get_date_as_list)

    checkin.rename(columns=lower_col_names(checkin.columns), inplace=True)

    session = connect_to_astra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.checkin_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.checkin_full(business_id text, date list<text>, total int,PRIMARY KEY(business_id))
    """)

    sleep(5)
    #### UPLOAD DATAFRAME TO CASSANDRA
    print('UPLOADING DATAFRAME TO CASSANDRA')
    checkin.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="checkin_full", keyspace="yelp")\
    .save()
    print('DONE')


def load_business():
    print('READING BUSINESS FILE')
    business = pd.read_json(r'/opt/data/initial_load/business.json', lines=True)
    
    print('TRANSFORMING ATTRIBUTES')
    atributtes = etl_atributtes(business)
    print('TRANSFORMING HOURS')
    hours = etl_hours(business)
    print('TRANSFORMING CATEGORIES') #FILLLED NA
    categories = etl_categories(business)
    print('GPS CLUSTERING')
    gps = etl_gps(business)
    print('FIXING RestaurantsPriceRange2 AND DELIVERY')
    atributtes['RestaurantsPriceRange2'] = pd.to_numeric(atributtes['RestaurantsPriceRange2'])
    
    print(atributtes['delivery'].unique())
    print('MERGING DATAFRAMES')
    data_frames = [business, atributtes, categories, hours, gps]
    full_data = reduce(lambda left,right: pd.merge(left,right,on='business_id', how='left'), data_frames)
    full_data = full_data.drop(['attributes', 'hours', 'city', 'state', 'categories', 'latitude_y', 'longitude_y'], axis=1)
    full_data.rename(columns={'Home Services':'HomeServices','Beauty & Spas':'BeautyAndSpas', 'Health & Medical':'HealthAndMedical','Local Services':'LocalServices', '7days':'SevenDays'}, inplace=True)
    full_data['mean_open_hour'] = full_data.mean_open_hour.astype(str)
    full_data['mean_close_hour'] = full_data.mean_close_hour.astype(str)
    full_data['RestaurantsPriceRange2'] = full_data.RestaurantsPriceRange2.astype(str)
    full_data.rename(columns=lower_col_names(full_data.columns), inplace=True)
    print('CONVERTING TO PYSPAK PANDAS')
    full_data2 = ps.from_pandas(full_data)
    session = connect_to_astra()
    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.business_full;")
    print(f'FULL DATA COLUMNS:\n{full_data.columns.to_list()}')
    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.business_full(
        business_id text,
        name text,
        address text,
        postal_code text,
        latitude_x float,
        longitude_x float,
        stars float,
        review_count int,
        is_open int,
        good_ambience int,
        garage int,
        BusinessAcceptsCreditCards int,
        RestaurantsPriceRange2 text,
        BikeParking int,
        WiFi int,
        delivery int,
        GoodForKids int,
        OutdoorSeating int,
        RestaurantsReservations int,
        HasTV int,
        RestaurantsGoodForGroups int,
        Alcohol int,
        ByAppointmentOnly int,
        Caters int,
        RestaurantsAttire int,
        NoiseLevel int,
        WheelchairAccessible int,
        RestaurantsTableService int,
        meal_diversity int,
        Restaurants int,
        Food int,
        Shopping int,
        HomeServices int,
        BeautyAndSpas int,
        Nightlife int,
        HealthAndMedical int,
        LocalServices int,
        Bars int,
        Automotive int,
        total_categories int,
        SevenDays int,
        weekends int,
        n_open_days int,
        mean_total_hours_open float,
        mean_open_hour text,
        mean_close_hour text,
        areas int,
        PRIMARY KEY(business_id))
    """)
    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    #full_data2 = full_data2[full_data2['business_id'].isin(more_reviews)]
    full_data2.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="business_full", keyspace="yelp")\
    .save()
    print('DONE')


def load_review():
    print('READING REVIEW FILE')
    review = ps.read_json(r'/opt/data/initial_load/review.json')
    #review = review[review['business_id'].isin(more_reviews)]
    print('DROPPING DUPLICATED ROWS')
    review = review.drop_duplicates()
    print('NORMALIZING DATES')
    review['date'] = review['date'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')

    review.rename(columns=lower_col_names(review.columns), inplace=True)

    session = connect_to_astra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.review_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.review_full(
        review_id text,
        user_id text,
        business_id text,
        stars float,
        date text,
        text text,
        useful int,
        funny int,
        cool int,
        PRIMARY KEY(review_id))
    """)

    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    review.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="review_full", keyspace="yelp")\
    .save()
    print('DONE')



def load_user():
    print('READING USER FILE')
    user = ps.read_json(r'/opt/data/initial_load/user.json')
    more_reviews_users = ['qEEk0PuoH1dVa619t8fgpw','EBa-0-6AKoy6jziNexDJtg','ZGjgfSvjQK886kiTzLwfLQ','JYYYKt6TdVA4ng9lLcXt_g','pitYOVSsF8R1gWG1G0qxsA','8fPlzYWo0j_nQrJMeyF0Fw','TGgfqWnUaCf6DM7TLuNhDQ','ZaMB7VbOwaARjxdhXjODxA','QJ-ikvhuRcigSCAWJTrnqQ','1xS8Jj23zHx8axIVopG3wA','Zn8uhC3DjoKjjwBiuM8oQg','ftRgzVFzv6-TOCBXEOdWeQ','VMtyZjaEJB9nfmjr4xdVlw','cARxOd_5yKCgsCbUZ5ED4Q','BBTexIhkFIYnS0rd56vsKQ','bme7nh1NwRfF4U34TXFK2Q','HVl561l9Y3jwMKNKnoxOyg','LcqNuhqaYt5ekKzaRirmIg','_l0csyXqNIcb3vG-1qR8DQ','E1olskL76b9TrcClgD9oBw','xjQSpme1Z7Xw8XehRLpYuA','86Fgk1s0tAVioJtVnJeBHg','XeS-0ONS5uoR_OfgZQebrA','Q_ixtOGUKUbCQ_sXThQAwA','cWAKzsMt0iMjBNPuNJVgDQ','n2pX5Ae8xCUi2_WlwcTkXQ','RHpCcdTbnBkgpdTYvbbTcw','0WqEkKMu03irkMiEtsFxZg','0MeivhX0kZCfV3zMtHtk9Q','afX9bbJ01xYU4L3qKbACUA','v8aeoMqdrClqfZxNB4lWCA','A9Q-_QpJy1mHjlwwP2RwJg','KLS_AWthM9n6KLcBTCF_RQ','GU_4tHnCYE6aKGU2XH-zkA','INUDMj7EmrLlTh6qDNprAg','7ktyPHE-NGnWxarOqjIQiQ','1Y38tVMSPH8jwIGw_y_E3g','I-wqWdMhZ-cgT4YPxY20gA','tt2qIFKZumubxk_UwXhijA','uJM_dR32MnrPztZZaC1rSQ','5BL_mcIfYvU9L08syGCfEg','c8qFkI_VusWo0xZvkjfBWQ','XuadFePn8P6l5epbEFnfKw','CJ1qOThxVs_V6VTYRL4Eaw','v62bgesQ-CSGDS1rNqXZsw','wKmJrv-p7gI6kBvlXQbcFw','Ebp-eYVK8Jtq-toYAHdDHA','UM1ApB4_5GJcxfaYS8jD4Q','8RcEwGrFIgkt9WQ35E6SnQ','gaqPcK4kIN9_N7htRQhTtg','A3gMFX-PQNR0aomxaDGJkw','yYTykS0TpVvhAtm0tda1sg','ij0Mu58AKlflicmBhI-xMA','mp3GeUkEr1gmMV5ZThtBpA','kcnmjaBiItk7wqk9kLiupA','X8tAoBJPDqEhwwNuCQVa0w','dQeH3N509e8AzzoC9Xdy5w','wulZapNwEvd8JA63oxTVKw','vIr2vYWApBXiQv5qHFn6gQ','bdIueXy-Sc0DfqzXbm9AEg','nc0pxjUX3IUM2diqr5ME4g','SQtIhJLfyPCSQvoA-qy4uA','Z_9WJOW0l_4jROfkkQWT4g','3fsfZqiRJKjEAiLIBbxsWA','sZuL9iA687GMN6AY3XBCIQ','d82e3N5c24CDA9Svi_t0fg','cHVfkm_h_vKt-ZIWMYQcrg','kFQ0u8c6x76jTwmEPmKoWw','6FVPYJedLt3FTCQlamuscw','1NOyCzEPeO6D7_2zHztOUQ','qVEOVvc9gB29v7IAWsnA_g','mkEqJ61lS7gJUR6nqlbRkw','1o4iynPusBLozi0CpLGH5Q','mgkPEU9hRPb9WnDwx3Z5iQ','8Rb0XNjTenrhJgLR2ydStg','lbj1atSiHxGFrzN9WAeynw','FV3oC7l8P_JWKULr2tnLFg','z3nv7VqIQEm8I7MTDwM-Ww','1WE0E1ewe2ZZU0l3V5vNWQ','mzdYdUsMk1uzl7kYyF7HaQ','94ICh-yDZXOOGu-e33C6RA','mqCL4lkyQLiNXj_74-tMvQ','nFiYurEbPZBAOCURMrBgag','jYknm57qpeZzTueiVIYTOA','K4myo2_Z_2KKdORpfdKoTQ','a6jqAsvBRT2mCkLhNjEY2g','vKGevla8lDHp8yxq4Poyfg','Hs4HViBxhjOkUJlthNKNWQ','uinzvCuEMwTRPcmgDtlAsg','KGIBBWFmyxnsc_-Ni0knmA','yrAX4ikJePO6eSN6_g6buw','PCISmI0atmUGCauThxGkPA','HYlb7vXGuiTvR5eiwSLBoA','leCO_9EFERggbuUoLYZ4gg','EmPcdONs6821YhkRQ8s87A','3zeNqvWimKiA9gOSbx2e6Q','hmVE7u0Nt45j5sZCH9j_YQ','uPdFJ22SxG74yhsatFVvrw','gcFwdhZfdhIacWQYbaLcBg','VwGzHDCtP9aZgXrUmgAajw','H88uWBzHemj6ldYJKU9-PQ','JclBkdNeLD0hpv9AR_HaeA','IwknZ7P0VWHSxOGsf4uMxw','JQe6xrPnYWZ3rWDbgwWZsA','DWASYCVz1yN6ytAFzfgEMQ','VZWt5nqlsjMntB2Ga97H5A','D-hz14YhfL_5d-BhcD02kw','HTFyT2aSIUUNYhw6fR99og','--4AjktZiHowEIBCMd4CZA','M9L3vxwilf57pxIk7xlQkA','BlU_lOa_o4FOG5wlvxRWpA','nXF3s0JyuSkxc01GaQLvOA','yU5TuZiKiG7VTgNGlffsYg','kaa6i7IHMyYIom7DIul9EQ','3A_Al7CYUQoLxK4CtnyabQ','5u5tto7PC3a7FQhIWEXflw','zq2nd0H1dVzi3bta4Zepuw','gbt3Ft7YmAmaxc8b27QM6Q','kQIp9HBSCjVfoGGV2wPLQg','MpBDUJXrry00iPnX5hbQGA','k6GurG0wmHVyiZGzmxs8eQ','KfPVXIzWw34sp9l5pfBE5Q','DQx7XI6ZxubfMndzFY22zQ','Vcbxj8JlIXkWgdvroc1qAg','6-DBwoNPw5zenYYSg6RrhA','WKJoektrmcMssSoQSI59ow','V0Kg9fkgfoCNvYpNJcv0Ng','hPuQIudnLHBJaJL7swpB8A','VZSLr8PCzqeh31HU8D9hXA','xKfuwsPWEaJwQiGaTYHelQ','Fq5CvUC1AzK8GSmpyn0UeQ','Khecid8LZRq9n6xe4qjgLA','h6uPago5wJSV4nPBO71iRg','IDKqeErl2l-huYugUzea6A','RGOJ68eoobRsYW4LgQtBcA','DGJRD862ewpN3Ux2pkZkHg','3PunMPL-ne29d2Oeq2PpXw','y5wLD2bvZ91fZnNid8Yh5g','ApOMEpkxD9yJTaaTxMfttA','v5PBVjPxj_W4-x89Lbc-TA','GQr0P3dgOZGSM3PYPoEtOg','HPlUnYMqvb5sGmX_paxCNQ','HXKZYi1d24WdafgmKhVUow','698q2zdsA2i9fYN1YhX1Xw','2anfbsmeoqeMYCiNwUow9Q','U8WHL_eN_XYntCINBrjThg','QsByY2gW90viW7wiqWeEcg','gyAfijhxmJzvwxPD9y6RHg','oh1jk_g-LqpKybskPYoP0Q','DtoCCg1vpf_Mj5Qk63zk3w','_w3Zqx8DiAEw6hT980Upvw','vxw6ilzQ9gKIfxLP3GvVrQ','eefUAeYwjVLTDu1iRxn7Iw','4gBvBdTybB8yydxh1nhOeQ','dhQOA8-FdpA6D0hNhXEYIw','Ytxn9vgPaA6X_1dKO_WNSw','SUV5QErmlMLhwsIkdUzK4w','TS_3tnBpHnlBYAoe978Xww','RbT0tdUrDiazlTGaORWGhQ','RsI5Sjjv1-EYue9yVN6WOw','hO0vrPqf4Ma2foebnR44Iw','bSRQp1PBDzCKyvepEQSYxQ','GrCKTYu9qPiUlowByh3VAA','2_1YJigBXrIll1fjVDS30w','eILPNE5BR9yaS1R3jlks9w','8uX0U8FyL9-HwNa64m1FMA','IjaFggQWriqk2rWv6RpPvg','IzkWFIoq-8k4wX_hV71Qbw','3iRkz8HR7qorxpx9bifywQ','QIogFooFd7UTIEx5iIU5kw','b7Ab6SBEvffhOX836lxATA','AjdePuDfMJTHphuJCQBdSQ','XBNtqhc72X57VK8GLNbsGg','_rWCX5E8R_3Ufv9JetOtHw','H6sRbrU6hePN89irFWBKew','tekHDsd0fskYG3tqu4sHQw','AxLCLK3Hxv-E7EntE3Y5gw','MxlkrZRxrSdJsPRdg1521A','ua3zAwmTMGvWdBR_rrjuzA','7BtVy5wVDth3-lwFbUgDXA','1GnIzms3F3MJJK2kyIwuHQ','y9dE4Q3D2Pu7dvVGdd-9Yw','UrKgCQvSTSUay1GNCkjnhA','HSQWc-Rgc_OpUTdKmSLjkQ','5UJ1ToXJ1Q4hpINbHcOJSQ','TA-wRnDB3Xv7_pSajJIUBg','jDWCDWUujfFpChAX8rgWYg','mPE3PKQ947F92s-4XvhQag','6UEJvMhHYIDzXI51lHPiIw','Usrg_bh_e7vJVwd0EMvkOw','a4WIm73rjfz1Z9_QGCGmaw','h4B9ZDALqcGev5XPyZ-Pow','iBQKwkuDvAdTM5gLWHgZwg','W1b5tQZzFN5KinOwypQDig','aDWWdGPZTyFkMGNyEwaVZg','bdfx_mqNeiYm-UKAkgRnRg','TBRQIFGGWjR7Z5Ki5_PfxQ','1-SMe3meQDCUyOypIS28PQ','dod4blQ8cwm3fdE00YFC0A','QBUFj_B6BH3QKzZpYHJz5w','zqCiUyWGnh633-2vLmuemw','88Ij88GMC7tcIegOicRdYA','4_IJKLhoyof73pvRal4Exw','vupwwaLgP0sVb6Qrc3Arvg','2kH_cKV0s_uZEIlCoHnzvw','W5fchLNpH6GmiVxx24zXgg','Sa9Jbde7d2kuzhVEAMRWEg','CfhitdMbQ88r9nlalyEb4w','Mv779di54QwcI43AO2mPLQ','xLcwDhFKONBP2F16lsaB_w','29UB_wrnUIdsxV2ZmrlZSg','zF_unBFpdWMY6el-B7wO8A','K1Cfhs2mESLD1nUWWfkFwQ','0qaUsn4TGgJYfbSxg_Fp2A','aAvl82YfsPmnNOMSMD8jBQ','Xjfc1owyGoFcM94kRxOCJg','fvgyz-tffn87IqaVkkN_DQ','NYxNe3Q5ROXBe7Fatst52g','GdcVn9hdpRa_rlYiEFaN3w','8i0Do65MQwRG4HAzO_detw','5i-nwtUBzji5An5fr4cpgA','m1q4gB6wdawoQXjPbs29mg','TDCb1p7KCiOd71tkY2rJ5g','4HzAp_Ck-hNIK7f4hEyKXA','BeS9uhpmc6Icap_Eso9Lqw','G3UtIVC0Sl8cbiA0_cPyMg','MvjdQ7T9YiBC9bL9fMaN3Q','ywH87wgOyI6ttRh0EgEeHg','CNmCBh1WQs-5a8RwIq0sMg','91z9RokpfMdwbxkdAA3-nw','aVwveJmgYeeN-ZnDX_shBw','IAReqg4gfz2OHz84p-qQ2w','ey88ywOdRVGEMmd6YyJNIA','tCti1lZLnkt5TXdOEdXqoA','7QTh-fkw9Nr2lO10-PV8yw','GQgH--hxKJisGmfsKj0xDA','zfvjfJ3RJR80A5D81emi4Q','NVxCOYSTlcRpnxaGop9N_g','Wjlo5YcDe2Lc7I4WNqooDw','7SgM5DrVSfkaqCpyeKjT0g','GlprrSVlHYCjhQtK1_LB6Q','337n8uBnr1pEP69JzVWDCQ','ft-zQNrmSXpG2heoTorC2w','UgYUeboBoETAgxom9iRdJQ','E_SoAPv9OT-eEzDZPoz2Tg','fQ2H73hk2r8vTeh-z7EhbQ','q8lvniKIxLt-d9zQ4yeEVg','HHkcBGeuKN55Ka5aULarmw','YqZniLami3yOXF3CSVxaFQ','7ziWZULyiZv2TesYNMFf4g','zDj-OYDs0dHjg2aXcVxD9A','IE4xKfypWuM3eaaq5q9IxA','xKsTgilmON3UrhEpM5Zl2Q','5Q5aeMq_dgomcVaSuvN5Lg','3R43ydfitOG0B4e_vuQyfA','6wRHe58b9X1mP7C86agykw','h59iX3YOfP1laPF000BV0Q','UnfeDunSlKcxvy2LPlzYBQ','AVNQyhynAB5z6iuN7P3Jfg','4LbxeQIyej3mr6HYYTsd7Q','WQ-_hGAsrAeR9QrZ4HH2lw','mDFeDwPLtG2TERrsR9kudw','uR4wrcLxQ7VUGBjM00iHNQ','5idrssvX83bcWhz11o4e1A','xW2A0MciHB0pLB4RHTi0nw','Q6vpjOTgGziXBkyURnUOqQ','wiIfZ8T2JdsRb8OKmdQj0Q','S7bjj-L07JuRr-tpX1UZLw','Nx-pMoHLNgB9ZVE3vBP05w','4reeeVHHWyZOp_Ejq5n40g','mSyINFt6TtXX9OHbwJfN5Q','yBkMMjXlQCEl7hiIGhQYgw','8Egic8Gg5qH0zC0mzB8BQA','rPzhe3wPt_68L3wrEhFg7g','d7qDp1KKg7GOLUMJLrlUsw','SjiTI7mX1UKdJbPLYeGI_g','3ywIG2FLBr9JNp6dnc8u3g','dEfQnSNy5US5FT1o5CMRbg','DaqsvhFFWRPMdLkriySHbg','acgJkn8BuKbBiKK956mWZw','LqC5hNXcFVY-jcbyxi-tuQ','GltpBXG2dCiMvi-bxvJPHA','1PhM0nRN6WN6bZadHN5cWQ','srou4DkAZLbVsgAPGqzN-g','ppYEK9zY7Vlhdf855ehlHA','C7hvShEj1G--Rc0exx6seQ','PGkMTm3hiermfR6TE498bw','LQ2CDCzIkXrUMXnh-W9QSg','RUQkywD4HQv2hvfv69Qq5g','bdalWswHTzYC1RDBYLehgw','TMMXLS-gEUQYDirHhL-9Og','yf7Wj4KMHh4Jv8Jwir8Kgg','QQwcp_Fmv7I3PLDAgUP1Mg','Oo_lq_YEKYfTSIO-1fnQvg','M6oZKl57_5bz0CeCCADdBA','-LpL5lWFDRytw5PADY1p5Q','P26U3x0gM5cG1s4gvWM82w','1Twew_ar_EPpb-NHSfcrog','azJSQWKdhUCsFELul6sVnQ','d5n7YWztGkm36c2LBQ8Y_w','oZ7y2hZHfnaoucaZC9Hp4g','NfjVkSRNironQBK0kHNoJw','DUNV27mVp5ATW6-J9LOJeA','8NjgVWDhLFNOTy19wJBoig','nfMgsrqADr_94KitluyCCQ','Du6bF8BUoJsIAuC0J3KLIA','xRJyMR1_QPuZuUhUuVaooQ','sMcCc4hcmnr_yolsz80gjA','mkQz8aihI1V19sYH8JQy-A','xSlBsTTPtPvwlclBIvAjYw','AO7thy9_PAXAT3rze_iaXw','c1XHqYtBl5qyeAspzPiCFg','sxGxgtlI6ipSimdUlSC0Fw','HmmBrh2_jdngGxjwzzzmdw','NiJmVx6JPcAUoE3d4j9aVg','ljqXU2O8o1Ofuu_CNRnXxA','RuYYtPAdhVjQ6u6GqmgZHw','6eErC2_WqGtCMO1tgqT1CA','CaSCl3j_tXFPhF6qzQciBQ','npaqVmBLp_bRk4DcraZYww','b9Q74D4XvFjJHKvLBLSaEA','1VJHfBEGw5xaEKcVSr38kQ','G35tigeQvOPqRVMzJVY9IQ','d-4Diz5jY05LPUE0mCBXlA','KJIzmEQ6peS6yc4RjlKS4Q','MVfbSEGqvhDWdmAfUeG38g','tNq35k92WAkSLSqeof03hg','aYQjCpAWtZMFS_Ce2ogvzA','CcXSCGzlhN8Of6xwafPkIQ','i2pV48tnyrf779rPkjnilA','H32QTRV87Q6CGTVUm1Z4QA','M7C_aSxzZyk93KNUmPUpYQ','k24QIVcmhhOnv9MCY3dELA','zxeXnjqmlrAspfk17LSZCg','mpnA1B30uMYGeuvKbJJgEw','6hNft_Qgu7T3b1X9B1-K5A','n1dDZfXcqw2A3iNwU5xlzg','WAzX3AaM6PQfzFJHaUF6xQ','2gWG2KZEhyIXXy5SOiUOZg','5BpTDdcXblUeG2dV-xciWQ','JRsGihBLWYVeeWRLP3in9A','qde7cw7YIJutTmRiFTEmfg','gqDbe1YwS4EhuCCNSNhpxg','mprkVxwLfSj6gW70USN7xA','41TYTuV87wmkLe-nrbwS3w','_1pVMv3nA38y5V9Cz8Zu_Q','QxVm4V55Dtdfv9Zfwm4daw','3dWsbFVG1c2iPzBM-WOQNA','39eSgXJRZwdWHs0IDAtzhQ','ovybrUwHdWRbFlKSM5H2Yw','kE8t6sP0yc4d47caMOyifQ','9IW9u_N2FfV-y5sUmLpYKQ','9ZiofeADDh_whZD8I9KDFw','g794-dhjC61kWSn1l3PoQA','-F6PdlUlQMGrsX-qug_f4g','KkmADj2xPhJL6DbZRVQcLQ','7xvUccpmp1fXWLAkZaXkDQ','G6ZnatT96yzdcX81PZyT3g','VEWzJnleYqTOVglmDALEyA','zFVBIOECiSpDb-e1WZOFVw','vzNYQaFyBnIU_bBkJsUJQA','Ng7RCiL-EjwAZo2jZoCwkA','5dXjCqtB1P4U3u6y8eEaZw','__1iEm-E3tvKuToIOCb76Q','IhM58htabC6OfgOdDyEW-Q','aD-xli-bVq6ABWuC_OqWIQ','riWDgbauId8TK7YFVgMNJw','uh3QEknKPHRG1wVABRYqWQ','3dcFnCZzBZrcKDeIYgiScw','J0Q7h3aSp4y1ElwA_JRAJQ','jlG7lVsF5LY2CxQUCGeQKg','0lSEYfAUQCqyBIaEQQUbbA','1OY-70r-JQaLbRbRBm04lA','9OJdZbS-8DiEtzqicUYQ8g','5dWx3ZbicVHtW7yGndsRoQ','4a7y9WOKYJmx5Hk_aUhmgg','i0FuzL8Aa5wV2O-n59_pEA','9woNV9FkJMX4MHygee84vA','BacuN4z2jKKPyBYV_wnAcA','xZ5TBpdWkHH15WTDgAqYFw','jheopFKRPEDAR_ur5HGTug','s16-BUo-orUsELvMu5ocKg','Wrdbwj4kmvlpptA0-ErOdA','yvQty5DgFfXlzZjnXteVbA','sruMjGMA3KbtDlMNvhwRLA','-G-s0z4MVaIjHIRAWp1myw','h8o3NmCcg3BAIHsBsm32Sw','xU5bD7aUKb9-LulOeBxCmQ','LrTOhEVai01haBAIMUY7mw','LimLRJP6n8pDPAT8knRhyw','0RJcckForPWsnVNMn_orYA','L8nFSWzvcsvLKKEekT5GuA','f2MiAt7rHnBq7LPL_9iqvA','zn_biH8_d_2j9wVgXj5Fsg','00hnvyIyIheNoyzhcOLZUA','A03MjVYocHO-6xOuk59ssw','WmfhKlnkeDYXwsY55xSO0g','9jV3AfGkcwPZNB1zR3jqbQ','Jd-ccxgw6bPWZMpiNNGijQ','XPzlQJDXbIPn71Hsgykugw','_l01jFciQKtGR1BuvrMG-g','ZnzpvxOa7eiLESrebeufJQ','q1MxLnDtvth41eaEogCeHw','Q7uJYSicQdI_RZbG8kjnSw','UYU71DRx-UDAee-eFOFfWw','R0w9KhptxXufhSt8dMNqnA','c1U0sTwSFFxShqB3leLoWA','0TL2UvlfvIe6g53JhjpiEQ','RNI2SJkPGW9L4Wm2YSxcMw','_XrNNcEbNnut2Q4Py-LfKg','60w9x_k88BtoVjwGLFn6nw','Lk6yr0Qff2ND7gzU075PRQ','ziIKYrsEnWOIogJhYJnVng','b9HV74ZB2neuy51hWstUGQ','dbELeRFW0v_bXybQNfb-_g','z3pethjEj75TLrX66FhUFQ','PCDorKmP5__w3BkyzjA2Xg','9UWf_0ohtl3_yqHdUjUe5A','wRfQaWiGKiR_htiSEkauSw','8z3dCuY4wurac7jhj7Oxew','ZOsDHReu1-GZuGgm9CeJQA','Rj88JTogoHMKg0wy9t3vhQ','YfvysNidwMsQtw56XAnGdQ','h6a1CAlAN-vm-JgwUoWeSg','adh20lqqWiv_cNhDfkPkNw','W9UO0ghofZM4WrFws44nGQ','v0JgXAAo2a1v5X2rSIpIPA','fkE8bjLL-fxYj-1_rlq28w','k6zYbMle3LI9jhMuY4Tc4w','q34MA5yPVVEkIV2mldl0jw','EEJZi7OjIY21ec-KXDa15Q','ySQP6km_KmfSQ48h1FxqLw','26F2byB18-dq7319tJHXrA','MksXytmVtf3oELjYMBlJ0A']
    #user = user[user['user_id'].isin(more_reviews_users)]
    print('DROPPING DUPLICATED ROWS')
    user = user.drop_duplicates()

    print('NORMALIZING DATES')
    user['yelping_since'] = user['yelping_since'].apply(transform_funcs.transform_dates).dt.strftime('%Y-%m-%d')

    # print('CREATING NEW FEATURES AND UPLOADING THEM')
    # influencer_Score_2(user)

    print('DROPPING ELITE & FRIENDS')
    user = user.drop(['friends', 'elite'], axis=1)

    print('USER COLS')
    print(user.columns)

    user.rename(columns=lower_col_names(user.columns), inplace=True)

    session = connect_to_astra()

    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.user_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.user_full(
        user_id text,
        name text,
        review_count int,
        yelping_since text,
        useful int,
        funny int,
        cool int,
        fans int,
        average_stars float,
        compliment_hot float,
        compliment_more int,
        compliment_profile int,
        compliment_cute int,
        compliment_list int,
        compliment_note int,
        compliment_plain int ,
        compliment_cool int,
        compliment_funny int,
        compliment_writer int,
        compliment_photos int,
    PRIMARY KEY(user_id))
""")

    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    user.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="user_full", keyspace="yelp")\
    .save()
    print('DONE')



def load_sentiment_business():
    sentiment = pd.read_csv(r'./data/sentiment_ok_unique.csv')

    sentiment.rename(columns=lower_col_names(sentiment.columns), inplace=True)

    sentiment2 = ps.from_pandas(sentiment)

    session = connect_to_astra()
    
    print('DROPPING TABLE IF EXISTS')
    session.execute("DROP TABLE IF EXISTS yelp.sentiment_business_full;")

    print('CREATING TABLE')
    session.execute("""
    CREATE TABLE IF NOT EXISTS yelp.sentiment_business_full(
        business_id text,
        neg_reviews int, 
        pos_reviews int,
        PRIMARY KEY(business_id))
""")
    sleep(5)
    print('UPLOADING DATAFRAME TO CASSANDRA')
    sentiment2.to_spark().write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="sentiment_business_full", keyspace="yelp")\
    .save()
    print('DONE')




#DAG de Airflow
with DAG(dag_id='Initial_load',start_date=datetime.datetime(2022,8,25),schedule_interval='@once') as dag:

    t_load_tips = PythonOperator(task_id='load_tips',python_callable=load_tips)

    t_load_checkin = PythonOperator(task_id='load_checkin',python_callable=load_checkin)

    t_load_bussiness = PythonOperator(task_id='load_bussiness',python_callable=load_business)

    t_load_review = PythonOperator(task_id='load_review',python_callable=load_review)

    t_load_user = PythonOperator(task_id='load_user',python_callable=load_user)

    t_load_user_metrics = PythonOperator(task_id='load_user_metrics',python_callable=load_user_metrics)

    t_load_sentiment_business = PythonOperator(task_id='load_sentiment_business',python_callable=load_sentiment_business)

    t_load_user_metrics >> t_load_user >> t_load_checkin >> t_load_bussiness >> t_load_tips  >> t_load_review >> t_load_sentiment_business