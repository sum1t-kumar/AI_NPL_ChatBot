import cx_Oracle

dsn = cx_Oracle.makedsn(r'********************','******',service_name='************')

con=cx_Oracle.connect(user="******",password="*******", dsn=dsn)

widths = []
columns = []
tavnit = '|'
separator = '+'

def sql_close():
    con.close()

def sql_print(cur):
    widths = []
    columns = []
    tavnit = '|'
    separator = '+'

    for cd in cur.description:
        # widths.append(max(cd[2], len(cd[0])))
        widths.append( len(cd[0]))
        columns.append(cd[0])

    for w in widths:
        tavnit += " %-" + "%ss |" % (w,)
        separator += '-' * w + '--+'

    results=cur.fetchall()
    print(separator)
    print(tavnit % tuple(columns))
    print(separator)
    for row in results:
        print(tavnit % row)
    print(separator)

def pull_winning_azid(azids):
    cur = con.cursor()
    querying = ''' select 'HCP' as Customer_Type, xref.AZ_ID as Given_AZID, HCP.AZ_ID as Winning_AZID from hcm_prod.C_B_HCP@MDMHUBP_HCM_PROD HCP, hcm_prod.C_B_HCP_xref@MDMHUBP_HCM_PROD xref
    where xref.rowid_object=hcp.rowid_object and xref.AZ_ID in ({ids})
    union
    select 'HCA' as Customer_Type, xref.AZ_ID as Given_AZID, HCA.AZ_ID as Winning_AZID from hcm_prod.C_B_HCA@MDMHUBP_HCM_PROD HCA, hcm_prod.C_B_HCA_xref@MDMHUBP_HCM_PROD xref
    where xref.rowid_object=hca.rowid_object and xref.AZ_ID in ({ids})'''

    try:
        cur.execute(querying.format( ids= str(','.join(azids)) ) )
    except Exception as cx_Oracle:
        return None

    return cur

def pull_acc_details(azids):
    cur_acc = con.cursor()
    querying = ''' select 'HCP' as Customer_Type, xref.AZ_ID as Given_AZID, HCP.AZ_ID as Winning_AZID, EXT.ID_TYP_CD_FK as EXTERNAL_ID_TYPE, EXT.ID_VALUE as EXTERNAL_ID_VALUE
    from hcm_prod.C_B_HCP@MDMHUBP_HCM_PROD HCP, hcm_prod.C_B_HCP_xref@MDMHUBP_HCM_PROD xref, hcm_prod.C_B_HCP_EXT_ID@MDMHUBP_HCM_PROD EXT
    where xref.rowid_object=hcp.rowid_object and xref.AZ_ID in ({ids}) 
    and HCP.rowid_object=EXT.HCP_KEY_FK and EXT.DEL_FLG='N' and EXT.trstd_src_flg='Y' 
    union
    select 'HCA' as Customer_Type, xref.AZ_ID as Given_AZID, HCA.AZ_ID as Winning_AZID, EXT.ID_TYP_CD_FK as EXTERNAL_ID_TYPE, EXT.ID_VALUE AS EXTERNAL_ID_VALUE
    from hcm_prod.C_B_HCA@MDMHUBP_HCM_PROD HCA, hcm_prod.C_B_HCA_xref@MDMHUBP_HCM_PROD xref, hcm_prod.C_B_HCA_EXT_ID@MDMHUBP_HCM_PROD EXT
    where xref.rowid_object=hca.rowid_object and xref.AZ_ID in ({ids})
    and HCA.rowid_object=EXT.HCA_KEY_FK and EXT.DEL_FLG='N' and EXT.trstd_src_flg='Y' '''

    try:
        cur_acc.execute(querying.format( ids= str(','.join(azids)) ) )
    except Exception as cx_Oracle:
        return None
    return cur_acc

def pull_Spec_state(spec, state):
    cur_spec = con.cursor()
    querying = "SELECT spec.spec_desc, address.state_cd_fk, count (distinct hcp.AZ_ID ) from hcm_prod.c_b_hcp@MDMHUBP_HCM_PROD hcp, hcm_prod.c_b_addr@MDMHUBP_HCM_PROD address, hcm_prod.c_b_hcp_addr@MDMHUBP_HCM_PROD addr, HCM_PROD.C_R_SPEC@MDMHUBP_HCM_PROD spec where hcp.rowid_object=addr.HCP_KEY_FK and  address.rowid_object=addr.addr_key_fk and  hcp.pri_spec_cd_fk=spec.spec_cd and addr.addr_typ_cd_fk='COMN' and addr.del_flg='N' and hcp.del_flg='N' and spec.spec_desc in (" + "'" + str("','".join(spec)) + "'" + ") and address.state_cd_fk in (" + "'" + str("','".join(state)) + "'" +") group by spec.spec_desc , address.state_cd_fk order by count (distinct hcp.AZ_ID )"

    try:
        #cur_spec.execute( querying.format( specs=str(','.join(spec) ),states= str(','.join(state)  )  ) )
        cur_spec.execute(querying)
    #cur_spec.execute("SELECT spec.spec_desc, address.state_cd_fk, count (distinct hcp.AZ_ID )                     from hcm_prod.c_b_hcp hcp,                     hcm_prod.c_b_addr address,                     hcm_prod.c_b_hcp_addr addr,                     HCM_PROD.C_R_SPEC spec                     where                     hcp.rowid_object=addr.HCP_KEY_FK                     and                     address.rowid_object=addr.addr_key_fk                     and                     hcp.pri_spec_cd_fk=spec.spec_cd                     and                     addr.addr_typ_cd_fk='COMN'                     and                     addr.del_flg='N'                     and                     hcp.del_flg='N'                     and spec.spec_desc   = 'REGISTERED NURSE'                     and                     address.state_cd_fk = 'FL'                     group by spec.spec_desc , address.state_cd_fk order by count (distinct hcp.AZ_ID ) ")
    except Exception as cx_Oracle:
        return None
    return cur_spec