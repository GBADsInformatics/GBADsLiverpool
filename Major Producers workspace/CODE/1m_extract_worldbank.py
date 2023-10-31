#%% About
'''
Downloaded from World Bank data browser, World Development Indicators table
https://databank.worldbank.org/source/world-development-indicators

'''
#%% Import and cleanup

# Read CSV
wb_infl_exchg_gdp = pd.read_csv(os.path.join(
   RAWDATA_FOLDER
   ,'worldbank_inflation_exchangerate_gdp_2010_2021'
   ,'20475199-8fa4-4249-baec-98b6635f68e3_Data.csv'
))
cleancolnames(wb_infl_exchg_gdp)
datainfo(wb_infl_exchg_gdp)

# Drop metadata rows
wb_infl_exchg_gdp = wb_infl_exchg_gdp.dropna(
   axis=0                        # 0 = drop rows, 1 = drop columns
   ,subset=['country_code' ,'time']      # List (opt): if dropping rows, only consider these columns in NA check
   ,how='all'                    # String: 'all' = drop rows / columns that have ALL missing values. 'any' = drop rows / columns that have ANY missing values.
).reset_index(drop=True)         # If dropping rows, reset index

# Rename columns
rename_cols = {
   'country_name':'country'
   ,'country_code':'country_code'
   ,'time':'year'
   ,'time_code':'time_code'
   ,'inflation__consumer_prices__annual__pct____fp_cpi_totl_zg_':'inflation_consumerprices_pct'
   ,'inflation__gdp_deflator__annual__pct____ny_gdp_defl_kd_zg_':'inflation_gdpdeflator_pct'
   ,'inflation__gdp_deflator:_linked_series__annual__pct____ny_gdp_defl_kd_zg_ad_':'inflation_gdpdeflator_linked_pct'
   ,'official_exchange_rate__lcu_per_us_dol___period_average___pa_nus_fcrf_':'exchangerate_lcuperusd'
   ,'price_level_ratio_of_ppp_conversion_factor__gdp__to_market_exchange_rate__pa_nus_pppc_rf_':'_drop'
   ,'real_effective_exchange_rate_index__2010__eq__100___px_rex_reer_':'exchangerate_effective_idx2010'
   ,'consumer_price_index__2010__eq__100___fp_cpi_totl_':'cpi_idx2010'
   ,'gdp__constant_2015_us_dol____ny_gdp_mktp_kd_':'gdp_usd_cnst2015'
   ,'gdp__constant_lcu___ny_gdp_mktp_kn_':'gdp_lcu_cnst'
   ,'gdp__current_lcu___ny_gdp_mktp_cn_':'gdp_lcu'
   ,'gdp__current_us_dol____ny_gdp_mktp_cd_':'gdp_usd'
   ,'gdp_growth__annual__pct____ny_gdp_mktp_kd_zg_':'gdp_growth_annualpct'
   ,'gdp_per_capita__constant_2015_us_dol____ny_gdp_pcap_kd_':'gdp_percpta_usd_cnst2015'
   ,'gdp_per_capita__constant_lcu___ny_gdp_pcap_kn_':'gdp_percpta_lcu_cnst'
   ,'gdp_per_capita__current_lcu___ny_gdp_pcap_cn_':'gdp_percpta_lcu'
   ,'gdp_per_capita__current_us_dol____ny_gdp_pcap_cd_':'gdp_percpta_usd'
   ,'gdp_per_capita_growth__annual__pct____ny_gdp_pcap_kd_zg_':'gdp_percpta_growth_annualpct'
   ,'gdp_per_capita__ppp__constant_2017_international__dol____ny_gdp_pcap_pp_kd_':'gdp_percpta_ppp_cnst2017_intldol'
}
wb_infl_exchg_gdp = wb_infl_exchg_gdp.rename(columns=rename_cols)
datainfo(wb_infl_exchg_gdp)

# Drop columns
drop_cols = [
   'time_code'
   ,'_drop'
]
wb_infl_exchg_gdp = wb_infl_exchg_gdp.drop(columns=drop_cols)

# Change to numeric
convert_tofloat = [
   'inflation_consumerprices_pct'
   ,'inflation_gdpdeflator_pct'
   ,'inflation_gdpdeflator_linked_pct'
   ,'exchangerate_lcuperusd'
   ,'exchangerate_effective_idx2010'
   ,'cpi_idx2010'
   ,'gdp_usd_cnst2015'
   ,'gdp_lcu_cnst'
   ,'gdp_lcu'
   ,'gdp_usd'
   ,'gdp_growth_annualpct'
   ,'gdp_percpta_usd_cnst2015'
   ,'gdp_percpta_lcu_cnst'
   ,'gdp_percpta_lcu'
   ,'gdp_percpta_usd'
   ,'gdp_percpta_growth_annualpct'
   ,'gdp_percpta_ppp_cnst2017_intldol'
]
for COL in convert_tofloat:
   wb_infl_exchg_gdp[COL] = pd.to_numeric(wb_infl_exchg_gdp[COL] ,errors='coerce')

datainfo(wb_infl_exchg_gdp)

#%% Describe and export

datainfo(wb_infl_exchg_gdp)
datadesc(wb_infl_exchg_gdp ,CHARACTERIZE_FOLDER)
wb_infl_exchg_gdp.to_pickle(os.path.join(PRODATA_FOLDER ,'wb_infl_exchg_gdp.pkl.gz'))
