from sqlalchemy import create_engine
import os

# engine = create_engine('postgresql://ubuntu:basicpassword@localhost/ba_employee_risk')

import psycopg2 as pg2
dbc = os.environ.get('DBDSN')  # Use ENV vars: keep it secret, keep it safe

conn = pg2.connect(dbc) #Need to connect to postgres first, so it then can create your new table
conn.autocommit = True  ## This is required to remove or create databases

cur = conn.cursor()
#Below lines substitute movies for the name of your database you want
cur.execute('DROP TABLE IF EXISTS risk_tables;')
# cur.execute('CREATE TABLE risk_tables;')
conn.commit()
conn.close()

##change movies here to the name of your database

conn = pg2.connect(dbc)
cur = conn.cursor()

##here is an example of one table -- id serial will automatically create a unique ID and autoincrement it

tables = """CREATE TABLE risk_tables (
ID                          INT PRIMARY KEY,
ClientRefNo3                  real,
DrvSurcharge                  real,
ClientRefNo2                  real,
OverRide                      real,
DrvAfterHours                 real,
ClientRefNo4                  real,
DriverCategoryID2               INT,
DrvExtras                     real,
DContact                      real,
PSpecInstr                    real,
Phone                         real,
Comments                      real,
SpecInstr                     real,
RoundTrip                     real,
toValidate                    real,
driver_extra_charges_avg      real,
sValue                        real,
Caller                        real,
PContact                      real,
Status                        real,
PPhone                        real,
DSpecInstr                    real,
DPhone                        real,
DrvPackage                    real,
ClientRefNo                   real,
MiscCharge                    real,
Email                         real,
sWeight                       real,
MileageTotal                  real,
DefaultDrvComm                real,
PZone                         real,
DZone                         real,
ClientSpecInstr               real,
DepartDate                      INT,
DrvCommTotal                  real,
state_is_NY                     INT,
sPieces                       real,
zone_diff                     real,
IRT                             INT,
start_q                         INT,
start_month_quarter_second      INT,
start_season_winter             INT,
birth_decade                    INT,
start_season_spring             INT,
InsuranceCharge               real,
WeightCharge                  real,
start_month_quarter_first       INT,
start_season_summer             INT,
VehicleId                       INT,
start_month_quarter_fourth      INT,
start_season_fall               INT,
state_is_NJ                     INT,
start_month_quarter_third       INT,
DrvWaitTime                   real,
PackageCharge                 real,
CareerTotalDaysWorked           INT,
CareerTotalDeliveries           INT,
risk_scores real
                                    );
                                    """
cur.execute(tables)
conn.commit()
conn.close()
