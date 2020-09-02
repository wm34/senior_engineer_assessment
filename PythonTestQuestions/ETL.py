import os, re, petl as etl, openpyxl, pyodbc, glob as g
from datetime import datetime

svr = '[SERVER_NAME]'  # e.g. 'WIN-4RQ5SVVZX34'
db = 'PersonDatabase'
conn_string = 'Driver={{SQL Server}};Server={0};Database={1};Trusted_Connection=YES'.format(svr, db)
conn = pyodbc.connect(conn_string, autocommit=True)

# create target tables
cur = conn.cursor()
DDL = """
    IF NOT EXISTS (SELECT * FROM sys.tables where name = 'Demographics')
        CREATE TABLE Demographics (
            FileDate char(6) NOT NULL,
            ProviderGroup varchar(50) NOT NULL,
            PersonID int NOT NULL,
            FirstName varchar(50) null,
            MiddleName varchar(50) null,
            LastName varchar(50) null,
            DOB datetime null,
            Sex varchar(10) null,
            FavoriteColor varchar(50) null
        );
        
    IF NOT EXISTS (SELECT * FROM sys.tables where name = 'ProviderFiles')
        CREATE TABLE dbo.ProviderFiles (
            id varchar(50) NULL,
            first_name varchar(50) NULL,
            middle_name varchar(50) NULL,
            last_name varchar(50) NULL,
            dob_1 datetime NULL,
            sex varchar(6) NULL,
            favorite_color varchar(50) NULL,
            attributed_q1 varchar(4) NULL,
            attributed_q2 varchar(4) NULL,
            risk_q1 float NULL,
            risk_q2 float NULL,
            risk_increased_flag varchar(4) NULL,
            loaddate datetime NOT NULL,
            filedate varchar(6) NOT NULL,
            provider_group varchar(50) NOT NULL
        );

    IF NOT EXISTS (SELECT * FROM sys.tables where name = 'AttributionRisk')
        CREATE TABLE AttributionRisk (
            PersonID int NOT NULL,
            Quarter char(2) NOT NULL,
            AttributedFlag char(3) NOT NULL,
            RiskScore decimal(15,12) null,
            FileDate char(6) NOT NULL
        );
        
    IF NOT EXISTS (SELECT * FROM sys.tables where name = 'ValidationErrors')
        CREATE TABLE ValidationErrors (
            name varchar(50) NOT NULL,
            row int NOT NULL,
            field varchar(50) NOT NULL,
            value varchar(MAX) null,
            error varchar(50) NOT NULL,
            loaddate datetime NOT NULL,
            filedate varchar(6) NOT NULL,
            provider_group varchar(50) NOT NULL
        );
        
    IF NOT EXISTS (SELECT * FROM sys.tables where name = 'ValidationErrorsLoad')
        CREATE TABLE ValidationErrorsLoad (
            name varchar(50) NOT NULL,
            row int NOT NULL,
            field varchar(50) NOT NULL,
            value varchar(MAX) null,
            error varchar(50) NOT NULL,
            loaddate datetime NOT NULL,
            filedate varchar(6) NOT NULL,
            provider_group varchar(50) NOT NULL
        );
    """
cur.execute(DDL)


# ex: 'X:\\privia_family_medicine\\from_client\\'
src_path = 'C:\\Users\\wmiller\\Documents\\Career\\Privia Health\\PythonTestQuestions'

for pathname in g.glob('\\'.join([src_path, '*.xlsx'])):
    fname = os.path.split(pathname)[1]
    provider_group = fname[:-12].strip()
    month = fname[-12:-10]
    day = fname[-10:-8]
    year = fname[-8:-6]
    filedate = ''.join([month,day,year])
    loaddate = datetime.now()

    # extract provider file data from xlsx worksheet
    x0 = etl.io.xlsx.fromxlsx(pathname, min_row=4, min_col=2)

    # transform and cleanup
    x1 = etl.convert(x0, {
        'Sex': {0: 'M', 1: 'F'},
        'Middle Name': lambda v: v[:1],
        'DOB[1]': lambda v: v.strftime('%Y-%m-%d %H:%M:%S'),
        'Risk Increased Flag': lambda v: v.strip()
        })

    # add provider group and file date columns and populate with values from filename
    x2 = etl.addfields(x1,[('loaddate', loaddate), ('filedate', filedate), ('provider_group', provider_group)])

    # update header: remove special characters, replace spaces with underscores, and make lowercase
    h0 = {i: re.sub(r"[^a-zA_Z0-9]+", ' ', i.lower()).strip().replace(' ', '_') for i in etl.header(x0)}
    x3 = etl.rename(x2, h0)

    # validation
    header = etl.header(x3)
    
    constraints = [
        dict(name='id_int', field='id', test=int),
        dict(name='dob_date', field='dob_1', test=etl.dateparser('%Y-%m-%d %H:%M:%S')),
        dict(name='sex_enum', field='sex', assertion=lambda v: v in ['M', 'F']),
        dict(name='attributed_q1_enum', field='attributed_q1', assertion=lambda v: v in ['Yes', 'No']),
        dict(name='attributed_q2_enum', field='attributed_q2', assertion=lambda v: v in ['Yes', 'No']),
        dict(name='risk_q1_float', field='risk_q1', test=float),
        dict(name='risk_q2_float', field='risk_q2', test=float),
        dict(name='risk_increased_flag_enum', field='risk_increased_flag', assertion=lambda v: v in ['Yes', 'No'])
    ]
    p0 = etl.validate(x3, constraints=constraints, header=header)
    p1 = etl.addfields(p0,[('loaddate', loaddate), ('filedate', filedate), ('provider_group', provider_group)])

    # load problems to [ValidationErrorsLoad] table
    etl.todb(p1, conn, 'ValidationErrorsLoad', 'dbo')

    # load problems to [ValidationErrors] table
    ValidationDML = """
        INSERT INTO dbo.ValidationErrors (
            name, row, field, value, error, loaddate, filedate, provider_group
        )
        SELECT
            name, row, field, value, error, loaddate, filedate, provider_group
        FROM
            dbo.ValidationErrorsLoad;
    """
    cur.execute(ValidationDML)

    # load provider file data to [ProviderFileLoad] table
    cur.execute("IF OBJECT_ID('dbo.ProviderFileLoad', 'U') IS NOT NULL DROP TABLE dbo.ProviderFileLoad;")
    etl.todb(x3, conn, 'ProviderFileLoad', 'dbo', create=True)


    # load provider file data to [ProviderFiles] table
    ProviderDML = """
        INSERT INTO dbo.ProviderFiles (
            id, first_name, middle_name, last_name, dob_1, sex, favorite_color, attributed_q1, attributed_q2, risk_q1, risk_q2, risk_increased_flag, loaddate, filedate, provider_group
        )
        SELECT
            id, first_name, middle_name, last_name, dob_1, sex, favorite_color, attributed_q1, attributed_q2, risk_q1, risk_q2, risk_increased_flag, loaddate, filedate, provider_group
        FROM
            dbo.ProviderFileLoad
        WHERE
            ISNUMERIC(id) = 1 AND id IS NOT NULL;
    """
    cur.execute(ProviderDML)

    # load demographic data to [Demographics] table
    DemoDML = """
        INSERT INTO dbo.Demographics (
            FileDate, ProviderGroup, PersonID, FirstName, MiddleName, LastName, DOB, Sex, FavoriteColor
        )
        SELECT
            filedate, provider_group, id, first_name, middle_name, last_name, dob_1, sex, favorite_color
        FROM
            dbo.ProviderFileLoad
        WHERE
            ISNUMERIC(id) = 1 AND id IS NOT NULL;
    """
    cur.execute(DemoDML)

    # load risk and attribution data to [AttributionRisk] table
    RiskDML = """
        INSERT INTO dbo.AttributionRisk (
            PersonID, [Quarter], AttributedFlag, RiskScore, FileDate
        )
        SELECT ID, aqid AS [Quarter], attributed AS AttributedFlag, risk AS RiskScore, FileDate
        FROM (
            SELECT id, attributed, risk, filedate,
                aqid = REPLACE(attributed_qtr, 'attributed_', ''),
                rqid = REPLACE(risk_qtr, 'risk_', '')
            FROM (
                SELECT id, attributed_q1, attributed_q2, risk_q1, risk_q2, filedate
                FROM dbo.ProviderFileLoad
                WHERE risk_increased_flag = 'Yes'
            ) AS cp
            UNPIVOT (
                attributed FOR attributed_qtr IN (attributed_q1, attributed_q2)
            ) AS att
            UNPIVOT (
                risk FOR risk_qtr IN (risk_q1, risk_q2)
            ) AS rsk
        ) AS x
        WHERE aqid = rqid;
    """
    cur.execute(RiskDML)
    
    #rename
    ##os.rename(pathname, ''.join([pathname, '.processed']))


cur.close()
conn.close()


