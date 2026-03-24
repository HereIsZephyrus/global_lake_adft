Deposited via The University of Leeds.

White Rose Research Online URL for this paper:

https://eprints.whiterose.ac.uk/id/eprint/210473/

Version: Supplemental Material

# Article:

Wang, H., Liu, J., Klaar, M. et al. (2024) Anthropogenic climate change has influencedglobal river flow seasonality. Science, 383 (6686). pp. 1009-1014. ISSN: 0036-8075

https://doi.org/10.1126/science.adi9501

# Reuse

Items deposited in White Rose Research Online are protected by copyright, with all rights reserved unlessindicated otherwise. They may be downloaded and/or printed for private study, or other acts as permitted bynational copyright laws. The publisher or other rights holders may allow further reproduction and re-use ofthe full text version. This is indicated by the licence information on the White Rose Research Online recordfor the item.

# Takedown

If you consider content in White Rose Research Online to be in breach of UK law, please notify us byemailing eprints@whiterose.ac.uk including the URL of the record and the reason for the withdrawal request.

 UNIVERSITY OF LEEDS

![](images/e77c461c3b4fde030c819cd6ad74f574d57b486be95a5233b32c3e9bbf15bb72.jpg)


![](images/e956a2d522770fd52a95fff5c5f9a7a67b8fcbb5573f10a7b3c9b9df2ebd0d32.jpg)


University ofSheffield

![](images/ff897ff7af62869c8255e068cb8dfdb1bf1de6016f44ff7b09d7273de0ef37d0.jpg)


fyork UNIVERSITY

# Supplementary Materials for

# Anthropogenic climate change has influenced global river flow seasonality

Hong Wang, Junguo Liu*, Megan Klaar, Aifang Chen, Lukas Gudmundsson, Joseph Holden

*Corresponding author: liujg@sustech.edu.cn

# The PDF file includes:

Materials and Methods

Figs. S1 to S18

Tables S1 to S3

References

# Materials and Method

# Observation-based datasets

Monthly river flow time series (calculated from daily records) were obtained from theGlobal Streamflow Indices and Meta data archive (GSIM) (18, 47). The Global Runoff DataCentre (48) (GRDC) database, offering river flow at monthly scale that are excluded byGSIM, are used as a complementary dataset. To compute RFS with minimal bias, twoselection standards were formulated: i) the study period ranges from 1965 to 2014 to ensuresufficient stations for analysis with wide spatial coverage; ii) monthly discharge is used tocalculate annual seasonality index only when there are 10 or more months of data available ina year. Given rapidly changing climate, we extended our analysis to include more recentyears by combining five regularly updated river flow datasets (Table S3) from national toglobal level for 2017-2019. All GRDC stations in countries that have a national or acontinental database (e.g. USGS data within the US) were replaced to avoid duplicated timeseries of river flow when combining datasets.

To achieve a global scale coverage, a recently published global gridded monthlyreconstruction of runoff (GRUN) data set was used (19). GRUN is developed from in-situmonthly river flow observations from the GSIM with a $0 . 5 ^ { \circ }$ spatial resolution covering theperiod from 1902 to 2014 (19). It is derived by training a machine learning algorithm basedon the gridded observations of precipitation and temperature from the Global Soil WetnessProject Phase 3 (GSWP3) dataset (19), therefore, GRUN is not able to explicitly account forthe effects of HWLU. Observed monthly river discharge from the GRDC dataset andmultimodel simulations from phase 2a of the Inter-Sectoral Impact Model IntercomparisonProject (ISIMIP2a) reconstructions are used for its validation (19). Four additional membersin the newly published G-RUN ENSEMBLE which overlap in 1965-2014 were used toaccount for the uncertainty of atmospheric forcing datasets on runoff, including runoffreconstructions forced with CRUTSv4.04, GSWP3-W5E5, GSWP3-EWEMBI and PGFv3(49). The spatial pattern of AE trends from G-RUN ENSEMBLE coinciding with GRUNsupports use of GRUN to conduct climate change detection and attribution analysis andfurther confirms the robustness of our results (Fig. S17). In summary, in-situ observationsincorporate the impacts from climate change (including ACC, natural forcing, and naturalclimate variability) and human activities (such as reservoirs, human water management, andland-use change, abbreviated as HWLU). Instead, GRUN and G-RUN ENSEMBLE onlyaccount for the impacts from climate change. To exclude impacts of reservoirs on the spatialpattern of RFS trends from in-situ observations, HydroBASIN subbasin units (Pfafstetterlevel 12) (50) are integrated with degree of regulation (DOR) provided by Grill et al. (51) todistinguish gauge stations into those influenced by reservoirs (DOR>0) and those unaffectedby reservoirs $( \mathrm { D O R } { = } 0 )$ ). The DOR at the subbasin unit level is represented by selecting themaximum value of DOR at the river reach scale. There are 6,150 stations identified as freefrom reservoir influence, while 3,914 stations are situated in subbasins or downstream ofreservoirs (with 49 stations located outside the HydroBASINS range due to their presence onislands, and another 7 stations lacking DOR information).

Snow-dominated regions were identified worldwide by the average snow to precipitationratio in the period 1979-2000 from the WFDE5 dataset (52), which contains globalprecipitation and snow flux at a resolution of $0 . 5 ^ { \circ }$ . Time series of snow fraction during 1965-2014 is calculated from the fifth-generation atmospheric reanalysis (ERA5) for full timecoverage (53). To rule out precipitation seasonality, observed monthly gridded precipitationdata from the Global Precipitation Climatology Centre (GPCC) (54) at a resolution of $2 . 5 \times$$2 . 5 ^ { \circ }$ for the period of 1965-2014 at monthly scale was used. Mean air temperature data fromthe CRUTEM5 dataset at a resolution of $5 \times 5 ^ { \circ }$ for the period 1965–2014 were used (55). The

permafrost and glacier maps are from the International Permafrost Association (IPA) andRandolph Glacier Inventory (RGI) (56, 57).

# Model simulations

We used the ISIMIP simulation round 2b (ISIMIP2b) outputs of global daily dischargeto investigate whether ACC impacts on RFS can be detected. Seven global hydrologicalmodels (GHMs) (CLM4.5, H08, MATSIRO, MPI-HM, LPJmL, PCR-GLOBWB andWaterGAP2) under the framework of ISIMIP2b were obtained (58). Each GHMs is run underdifferent climate scenarios with different social and economic scenarios in four bias-correctedglobal climate models (GCMs) contributing to the Coupled Model Intercomparison Project 5(CMIP5) archive, except for MPI-HM (only three GCMs, Table S2), thereby providing uswith 27 GCM-GHM combination datasets of gridded daily discharge. All models consideredwater consumption sectors (for irrigation / domestic / industrial purposes), reservoirmanagement, and land-use change, apart from CLM45 and MPI-HM, which only consideredirrigation water use without reservoir operation. The scenarios of GCM-GHM combinationsconsidered are listed below (Table S2):

1. Picontrol&1860soc: pre-industrial control (Picontrol, including natural climaticvariability) simulations under 1860 social and economic scenarios (1860soc) run from 1661-1860. All available Picontrol&1860soc simulations were split into non-overlapping 50-yearsegments, resulting in a total of 108 segments, to account for natural climate variability. Thissimulation is used in the subsequent climate change detection and attribution method.

2. Picontrol&HWLU: the Picontrol simulations run from 1861-2005 are used to driveGHMs that account for HWLU, which do not account for ACC. For 1965-2005, thesimulations are forced with histsoc (except for CLM45 with 2005soc). For 2006-2014, HWLU is kept at the constant level of 2005soc.

3. HIST&HWLU: simulations under historical climate forcing (HIST, includinganthropogenic climate forcing, natural forcing, and natural climatic variability) are used todrive GHMs that account for HWLU. For 2006-2014/2006-2019, the medium–high emissionscenarios (Representative Concentration Pathway (RCP) 6.0) is used to extend the studyperiod (38).

To understand the effect of soil moisture on RFS, monthly gridded soil moisturemodeled data from the Climate Prediction Center (CPC) soil moisture dataset for the period1965–2014 were analyzed. These data are monthly averaged soil moisture water heightequivalents with a spatial resolution of $0 . 5 ^ { \circ }$ .

# Seasonality index

After acquisition of data, both the reconstructed and modelled data were interpolated toa $2 . 5 { \times } 2 . 5 ^ { \circ }$ grid using the second-conservative regridding method from their respectiveoriginal grids. We assessed the seasonal variation of monthly river flow using an informationtheory metric known as Apportionment Entropy (AE). This metric is non-parametric and mayeven encompass high-order moments, in contrast to other seasonality indices based onstandard deviation, Fourie decomposition, and circular statistics (13–15, 23). AE is thereforevery well suited to analyzing river discharge distributions globally. Moreover, informationtheory metrics have been widely used as a measure of rainfall seasonality in both hydrologicand climatological contexts (21, 22). In our case, higher AE values imply lower seasonalvariation, and lower AE imply higher seasonal variation.

To estimate AE of river flow over the year k, we firstly calculated the sum of monthlyvalues $\mathbf { X } _ { \mathrm { m } }$ (m=1,2,…,12) in year k, denoted as $\mathrm { X _ { k } }$ .

$$
X _ {k} = \sum_ {m = 1} ^ {1 2} x _ {m, k} \tag {1}
$$

The AE at year k can be calculated as (20):

$$
A E _ {k} = - \sum_ {m = 1} ^ {1 2} \frac {x _ {m , k}}{X _ {k}} \log_ {2} \left(\frac {x _ {m , k}}{X _ {k}}\right) \tag {2}
$$

which by definition, when monthly river flow is uniformly distributed, river flow isequal in each month, and AE reaches its maximum, $\log _ { 2 } 1 2$ . In contrast, if the annual riverflow is concentrated in one month and there is no discharge for the rest of the months, $\mathrm { A E = }$0.

Anomalies of annual AE were computed by subtracting the long-term mean over the full1965–2014/1970-2019 period for each station or grid cell. All datasets were masked foroverlapping pixels between observational reconstructions and model simulations to achievethe same spatial coverage. We directly use GRUN runoff to calculate RFS, since river flowcan be assumed to equal the runoff multiplied by the drainage area (area weighted discharge)at a monthly timescale, where water losses through e.g. channel evaporation are negligibleexcept for in few very large basins (19).

AE characterized the magnitude change of the RFS. Our findings suggest that the trendsin river flow timing may not be significant at the stations with significant AE trends includedin our analysis, particularly at a monthly scale. This is because flow changes in no high-flowmonths offset the shift in the centroid timing of river flow (Fig. S5).

# Trend and reliability analysis

Sen’s slope is a robust and nonparametric method to reflect time series trends,commonly used in hydro-meteorological analysis to estimate linear trends (59). Stahl et al.developed a method using the Sen’s slope k to calculate change ratio expressed in units ofpercent change per decade to represent trend magnitudes (60):

$$
\text {c h a n g e} = \frac {k \cdot 1 0}{\bar {x}} * 1 0 0 \tag {3}
$$

where $\bar { x }$ is the mean discharge in the study period. This method is robust to outliers $( \boldsymbol { 6 0 } )$ .In addition, trend estimates from catchments with different sizes and climate are comparablewith this method (47). Significance of trends is estimated by the Mann-Kendall statisticaltest (61).

Previous literature suggested that trend analysis can be considered when at least $70 \%$ ofthe data-years for stations are available (47). However, long-term hydrological data aredeficient in high latitudes, where RFS is stronger. To overcome this lack of long-term station-based river flow observations, the length of record (LOR) method is adopted to characterizethe uncertainty associated with the application of shorter record lengths when data are limited(61, 62).This LOR analysis was used to determine how many years of data were required toachieve a specified level of statistical certainty for any flow gauging station (62). Here, a$90 \%$ confidence interval for data to be within $5 \%$ of the long-term mean was selected todefine AE uncertainty. To do this, the whole available period for each station was used toassess the variability of river flow AE and determine the length of record required incalculations when there are at least 35 years record with at least 20 years in the period beingstudied. Finally, the AE trend was calculated only when there are $\mathrm { i } ) \geq 3 5$ years $70 \%$ of the1965-2014/1970-2019 time period) available or ii) $\geq 2 0$ years but no more than 20 yearsrecord length was required to constrain AE to be within $5 \%$ of the long-term mean with $90 \%$

confidence interval. Typically, large rivers flowing through flat lowlands required shorterrecord lengths to represent the flow regime because their discharge is characterized withrelatively lower intra-and interannual variability than highland streams (62).

# Interpreting AE from high and low flows

To understand the trends of seasonal variability of river flow qualitatively, the annualmean trends of river flow were also included to help identify potential reasons for seasonalvariations of river flow for global regions. To interpret the results, low- (high-) flow monthswere defined as three calendar months when the long-term monthly means of river flow islowest (highest). Here, we developed a suite of six alteration metrics ascribed as $\mathrm { T } _ { \mathrm { L H } }$ (trendsof low flows and high flows) $\mathrm { ( L \mathrm { - } H ^ { \ast } }$ , $\mathrm { L ^ { - H + } }$ , $\mathrm { L ^ { * } H + }$ , $\mathrm { L ^ { * } H - }$ , $\mathrm { L + H - }$ , $\mathrm { L ^ { + H ^ { * } } }$ ) based on thechange directions of AE and the signs and significance of annual mean river flow changes,dividing gauges into six distinct categories. Notation – and $^ +$ represent the change directionof river flow in low- and high- flow month, * indicates that the change is not predominant.Only stations with significant AE trends were considered. Therefore, we excluded $\mathrm { L + H + }$ ,$\mathrm { L } { - } \mathrm { H } -$ , and $\mathrm { L ^ { * } H ^ { * } }$ as we assumed stations with significant AE trends would not exhibit thesame predominant changes in both low- and high- flow months.

The significant seasonal variations $( p < 0 . 0 5 )$ of river flow at each gauge can beattributed to the variations of high flows and low flows. For example, a station with asignificant $( p < 0 . 0 5 )$ increasing AE trend and insignificant $( p > 0 . 0 5 )$ annual mean river flowtrend can be assigned to $\mathrm { L + H - }$ (increasing low flows and decreasing high flows).Specifically, $\mathrm { L - H ^ { * } }$ indicates that decreasing low flows is dominant assuming that bothannual mean river flow and AE are experiencing significantly decreasing trends, $\mathrm { L } { \mathrm { - H + } }$indicates decreasing low flows and increasing high flows contribute to the significantdecreasing trends of AE with insignificant annual mean trends, $\mathrm { L ^ { * } H + }$ indicates thatincreasing high flows is prominent in the situation of decreasing AE and increasing annualmean trends, $\mathrm { L ^ { * } H - }$ indicates that decreasing high flows is prominent under the condition ofsignificantly increasing AE and decreasing annual mean trends, and $\mathrm { L } { + } \mathrm { H } ^ { * }$ indicates lowflows are significantly increasing in the case of significantly increasing AE and annual meantrends. A few stations with significant AE trends, such as $\mathrm { L + H + }$ in the upper Midwest ofCONUS and $\mathrm { L - H - }$ in southeast Brazil, are outside our classification framework.Nevertheless, there is still a predominant change in low- or high-flow months overall, whichwould result in a significant RFS trend (Fig. S5).

# Climate change detection and attribution

To quantify possible influences of external forcings in the observed/reconstructed RFS,we conducted climate change detection and attribution analyses on AE over the NHL (above$5 0 ^ { \circ } \mathrm { N } )$ ) over the 1965-2014/1970-2019 period. We used two methods to test robustness of theresults: one is a correlation-based method (17, 34, 35) and the other is the optimalfingerprinting approach (63) with a regularized covariance estimate (36).

The correlations between the multimodel mean and the observations / pre-industrialcontrol, that is corr(HIST, obs) and corr(Picontrol, HIST), respectively, quantifies thesimilarity between the estimated response to human-induced climate change and the observedresponse or a consequence of natural climate variability (17, 34, 35). The null hypothesis isthat there is no signal in the observations resulting from human-induced climate change, thatis, the corr(HIST, obs) will be approximately zero and not distinguishable fromcorr(Picontrol, HIST). On the contrary, if corr(HIST, obs) is significantly larger than zero,e.g. greater than almost all the estimates of corr(Picontrol, HIST), then the null hypothesis isrejected with high confidence. This indicates that the observed response includes a signalstemming from the external forcing given by human-induced climate change. A normal

distribution using the mean and standard deviation of corr(Picontrol, HIST) was assumed forproviding the $9 5 \%$ and $9 9 \%$ confidence levels in comparison with corr(HIST, obs) (34, 35).

For the correlation approach, all available Picontrol simulations were used and dividedinto multiple nonoverlapping 50-year segments with the last segment discarded if shorter than50 years to match the time span of our study period, providing 216 $( 8 \times 2 7 )$ chunks ofPicontrol simulations span 1661–2099 in total. It is noted that there is no difference if weexclude Picontrol&1860soc simulations in the correlation method (Fig. S18), since theimpacts of HWLU on RFS are underrepresented in simulations (Fig. 3C). The Spearmancorrelation coefficient was used because of its resistance to outliers.

We used the correlation method to examine the spatial and temporal consistency of AEchanges between the multimodel mean of historical simulations and the observation, asopposed to estimates from Picontrol. We did this by comparing spatial corr(HIST, obs) withspatial corr(Picontrol, HIST) of AE trends (%/decade), denoted as corrspatial(HIST, obs) andcorrspatial(Picontrol, HIST), distinguished from the temporal correlation coefficient of AEanomalies, denoted as corrtemporary(HIST, obs) and corrtemporary(Picontrol, HIST).

Optimal fingerprinting was applied to detect and attribute changes in the observationalreconstructed magnitude of the AE in the NHL (above $5 0 \mathrm { ^ { \circ } N }$ ) from 1965-2014/1970-2019.The optimal fingerprint method is based on the generalized linear regression of the observedor reconstructed AE as a combination of climate responses to external forcing plus internalvariability (36). The regression model for the one-signal climate change detection andattribution analysis is:

$$
\left\{ \begin{array}{l} y = x ^ {*} \beta + \varepsilon \\ x = x ^ {*} + v \end{array} \right. \tag {4}
$$

where observation vector y and the simulation ensemble average response matrix $_ x$are known, the actual regressor of $x ^ { * }$ in response to external climate forcing can be obtainedwith the noise term ??. ?? represents the effect of internal variability that remains in $x$ resultingfrom sampling since multimodel averaging of forced runs cannot remove all internalvariability because the size of the latter is usually small. The observations are acquired fromthe actual regressor $x ^ { * }$ by multiplying the scaling factor $\mathbf { \beta }$ plus the noise term ɛ ∼N (0,??), with $\pmb { \Sigma }$ being a covariance matrix derived from 108 $( 2 7 { \times } 4 )$ groups of unforced Picontrolsimulations under 1860soc accounting for natural variability and uncertainty of multimodelmeans. To derive the best estimate of $\mathbf { \beta }$ and the associated confidence intervals, $\pmb { \Sigma }$ is dividedinto two equally independent groups $\pmb { \Sigma } 1$ and $\pmb { \Sigma } 2$ following previous research (17, 35). Toaccount for uncertainty of randomly splitting Picontrol&1860soc simulations into two halves,we replicate the procedure 2,000 times, resulting in $2 { , } 0 0 0 \ \beta$ and corresponding $9 9 \%$confidence intervals. Median of the resamples was considered as best estimate of $0 . 5 \mathrm { - } 9 9 . 5 \%$uncertainty ranges of $\mathbf { \beta }$ . A signal is detected if the lower confidence bound of $\mathbf { \beta }$ is abovezero. Furthermore, if the confidence interval of $\mathbf { \beta }$ includes one, the magnitude of the meanresponse of AE is consistent with the observations. In this study, $x ^ { * }$ is estimated using theensemble mean of the HIST&HWLU simulations $( 3 6 )$ . If simulations include the drivers ofanthropogenic climate forcing, that is HIST&HWLU, are consistent with the observation,then it is possible to claim attribution. The consistency of the unexplained signal ε withinternal variability was also assessed using a residual consistency test (RCT) (36). The RCTuses a non-parametric estimation of the null distribution through Monte Carlo simulations,and its $p$ value is estimated. If $p > 0 . 1$ , the RCT passed, which indicates the consistencybetween the regression residuals and the model-simulated variability (36). The optimalfingerprinting detection and attribution analyses were performed using code provided in ref.(36).

![](images/7b262d54a8ead2a9f33bcc5fe31e99582a593bd389afd3ace22b003268ec7676.jpg)


![](images/07c1685bb5cced2a2bdc807ccd86036066a6cd101377b44fd8548605e924d5a0.jpg)


![](images/f03f97802c20498170232fb7c86a08d001f5c508c91b6d14e153868957214d8c.jpg)



Fig. S1.



Classification of river flow seasonality. (A) Distribution of low, moderate, and highapportionment entropy (AE), corresponding to high, moderate, and low river flowseasonality, respectively, based on 30th and 70th percentile of mean AE (2.91 and 3.28, twodashed lines in (B)) in the 1965-1994 baseline period. (B) Time series of low, moderate, andhigh AE corresponding to three types of flow regimes with similar annual mean river flow$( 4 0 { \sim } 4 5 \mathrm { m } ^ { 3 } / \mathrm { s } )$ in the stations of $\textcircled{1}$ Bogadinskoje, south Serbia; $\textcircled{2}$ near Fort Kent Maine,northeast CONUS; and $\textcircled{3}$ Rio Pardo, southeast Brazil, respectively. 30 years referencedmean AE are noted in the left corner. River flow observations are not available after 2000 inBogadinskoje.


![](images/f97017b4dd4d64ec16bee6ccdb01890eb0d0c6083f0d986041d9e50b26f408b1.jpg)



Fig. S2.River flow seasonality trends represented by apportionment entropy (AE) $\%$ decade-1) over50 years (1970–2019). Similar to Fig. 1A in the main text, but with study period replacedwith 1970-2019.


![](images/132915a70de6355fc5a5c996c3a87a360c4d74b915732fe08ca7760a2602db7f.jpg)


![](images/ebcb7e976a5d18a96c57dc1cf8623c3d3eb1bfca58070e96aa6856956f3860e2.jpg)



Fig. S3.Trends of annual mean river flow (% decade-1) over 50 years (1965-2014) in the stations with(A) significant $( p < 0 . 0 5 )$ annual mean trends (2301 stations) or (B) significant $( p < 0 . 0 5 )$ seasonal trends (2134 stations). In (B), stations without significant annual mean trends arerepresented as black edged triangles, which account for $65 \%$ (1380 stations) of the stationswith significant seasonal trends.


![](images/48a612109d2e86758d347dedf0c0fde976281523c2499760b9b9f997ee2415aa.jpg)


![](images/fede0b262c75750b2866c57bdb818385ad76a508dc7bfa95112f60cd9af1189c.jpg)



Fig. S4.Trends of river flow in (A) low- and (B) high- flow months (%decade-1) over 50 years (1965-2014). Stations with significant trends $( p < 0 . 0 5 )$ are circled with black. The number ofstations included is indicated in parentheses. Regions where snow fraction in precipitation islarger than 0.2 are showed in grey as snowmelt-dominated areas. The pie charts depict theproportions of stations with significant trends (hatched, $p < 0 . 0 5 )$ and insignificant trends(solid) worldwide (ALL) and in the snowmelt-dominated areas (SN).


![](images/2c54d0625b05795d945ddc95a85f1ebdf8bd1c826b8fa1860453fecf555ec817.jpg)


![](images/233520a19507f4945cfd5dfd24c3e845b1c148cb981fad8f15bb55fc39f5668e.jpg)


![](images/ea08caea8ed553b8339ad4e6cafd65ad7831a0150b3c081136bc379a3acda6f4.jpg)


![](images/9c6170769555e8db678f5d9d3b206388d4b39edbfcff4070e26118968240b92e.jpg)


![](images/a0fd7c8b1f9abb6c0428d1f9f3e57e51d79edd82252c5d78436ceabed8718027.jpg)


![](images/793184764ff04f33c5dc4a649afaea45f4773a9074543c9b4417ca318fc269ac.jpg)


![](images/22385cfba3399efff2848ce8ae2139c0e99f6153151aef593fbad6c426b786cd.jpg)


![](images/97e90099cc16dc8f59f7f76dcd2daabd648dc596644d8a8cf7d11d50089d5a74.jpg)


![](images/a0ffe3b5087d3cfb4459b30652f7208848fe0f37a797c137c55d74799835da35.jpg)



Fig. S5.


Normalized monthly mean flow regime (grey line) within the 25th and 75th percentile range(grey shading) and boxplot of monthly and annual mean river flow trends $( \% \ \mathrm { y r ^ { - 1 } } )$ in (A)northern North America, (B) northern Europe, (C) western Russia, (D) higher elevationEuropean Alps, (E) south Siberia, (F) Pacific Northwest, (G) upper Midwest, (H) northeastCONUS, (I) southeast Brazil. Low (high) flow months are defined as three calendar monthswith lowest long-term monthly means of river flow noted in red (blue). Only stations whoseseasonal trends are significant $( p < 0 . 0 5 )$ and the same as the dominant change direction ineach region are included in statistics. Numbers within annual boxplots indicate the number ofpositive and negative trends, excluding trends equal to zero. Numbers in parentheses indicatethe count of trends that were significant $( p < 0 . 0 5 )$ .

![](images/42c150903e3ffcc8834327c81c9b08ff490b9d4d39ef906c7e54e1a474f8d643.jpg)



Fig. S6.



Temporal evolution of river flow seasonality with their potential climatic drivers forsubspaces in the nine hotspots in (A) northern North America, (B) northern Europe, (C)western Russia, (D) higher elevation European Alps, (E) south Siberia, (F) Pacific Northwest,(G) upper Midwest, (H) northeast CONUS, (I) southeast Brazil. Data show anomalies of soilmoisture in high-flow months (purple), temperature (yellow), precipitation (blue), river flow(red) seasonality, and snow fraction (green) changes. Solid lines show the median and shadedbands indicate the spatial variability within the subspaces (25th and 75th percentiles). Bandsare not shown for snow fraction to enhance readability of the plot. Regions where snowfraction in precipitation is larger than 0.2 are shown in light grey as snowmelt-dominatedareas. Permafrost and glacier distributions are shown in medium and dark grey, respectively.All times series are smoothed by a 10-yr running mean and indexed to the middle year.


![](images/df05cd0565aabab221ac8c3511d19344f9ffa8772983caa2518150b0a698cb13.jpg)



Fig. S7.


Agreement of seasonality trends from 27 GHMs under HIST&HWLU. Fraction of GHMswith weakening river flow seasonality at each grid cell. The purple dashed line at $5 0 \mathrm { { ^ \circ N } }$highlights the boundary of the northern high latitudes defined in this study. Areas of annualprecipitation below $1 0 0 ~ \mathrm { { m m } }$ and Greenland are masked in grey.

![](images/8a0522967571df0c67bf9db65364d87757f319c7f0c849df29810fc0cd6a4b51.jpg)


![](images/6eb7d8a703aec0ce8612a0c2c946fc2fffa49958333c3e4a8572e76adc4d6634.jpg)


![](images/b534918737d54cd70e1e8edb7adb5dca8bca2434e5429b6a943ceefd8dd12ff1.jpg)


![](images/14156c88f03bda145c28e83803969deae99b119e24a68aae4eceaa77e8a78555.jpg)



Fig. S8.



Similar to Fig. 3A-3C in the main text, but with study period replaced with 1970-2019. Note(A) shows AE trends from CRU-TS, which is one observational runoff reconstruction drivenby the CRUTSv4.04 atmospheric forcing dataset in the G-RUN ENSEMBLE. (B, C)



Simulated changes based on multimodel mean that account for historical water and land use(HWLU) under either historical radiative forcing (HIST) (B) or pre-industrial control(Picontrol) (C). Areas with annual precipitation below $1 0 0 \mathrm { m m }$ and Greenland are masked ingrey.


![](images/838e81b78fa00646a9d5c3b8904af64cc0206c0666840a415989bc9c6720a604.jpg)



Fig. S9.


Observational reconstruction of river flow apportionment entropy (AE) trends (% decade-1 )for the G-RUN ENSEMBLE member driven with CRU-TS in 1970-2019. Black dots indicatea trend significance at 0.05. The purple dashed line at $5 0 \mathrm { { } ^ { \circ } N }$ highlights the boundary of thenorthern high latitudes defined in this study. Areas of annual precipitation below 100 mm andGreenland are masked in grey.

![](images/26552a5e64c004cdfb68be3eb2739a7063a59dbd2cf9226825ff7e5a62b9ec19.jpg)



Fig. S10.



Spatial Spearman correlation coefficient of apportionment entropy (AE) trends for 1965-2014(% decade-1) between the multimodel mean from HIST&HWLU and observed changes fromGRUN (corrspatial(HIST, GRUN), red) compared with an empirical distribution of correlationcoefficients from 216 chunks of Picontrol simulations (corrspatial(Picontrol, HIST), grey).Vertical blue lines mark the $9 5 \%$ and $9 9 \%$ cumulative probability of an assumed normaldistribution for the correlations.


![](images/059e0bc33ff938dc6e6ecbbbb0cdd5f8e994a5079508e1dd6eab849dde1f0f4b.jpg)


![](images/9b9865ca7a0aee810619faaff9506a8efe9cdc2f3f2af98ca8dbb3e7fa857481.jpg)



Fig. S11.


Similar to Fig. 3D and 3E in the main text, but with study period replaced with 1970-2019and observational runoff replaced with CRU-TS, which is one observational runoffreconstruction driven by CRUTSv4.04 atmospheric forcing dataset in the G-RUNENSEMBLE. (A) Global multimodel (mdl) mean time series of apportionment entropy (AE)anomalies for HIST&HWLU and Picontrol&HWLU response and CRU-TS observationsabove $5 0 \mathrm { { } ^ { \circ } N }$ . The red spread is ensemble standard deviation of HIST&HWLU, and thin greylines are 27 model results of Picontrol&HWLU. (B) Correlation coefficient of AE anomaliesbetween simulations with and without ACC (corrtemporary(Picontrol, HIST)) or observation-based reconstructions (corrtemporary(HIST, CRU-TS)) across $5 0 ^ { \circ } \mathrm { N } { \cdot } 9 0 ^ { \circ } \mathrm { N }$ . Correlationcoefficient between the mdl mean from HIST&HWLU simulations and 216 chunks ofPicontrol simulations with 50-yr segments are shown as an empirical probability densityfunction in grey. Vertical blue lines mark the $9 5 \%$ and $9 9 \%$ cumulative probability of anassumed normal distribution for the correlations. The inset shows the confidence interval ofthe scaling factor from the optimal fingerprinting method with $0 . 5 { - } 9 9 . 5 \%$ uncertainty range.A signal is detected if the lower confidence bound is above zero (the solid line). Theamplitude of the mean response is consistent with the observations if the confidence intervalincludes one (the dashed line). The residual consistency test (RCT) passed $( p >$0.1), indicating the consistency between the regression residuals and the model-simulatedvariability.

![](images/a354d1918874abe568a3501d0a09f761ae7771d6bf34f2e4f245b5f12923be58.jpg)


![](images/d43a850796ee60e0313cf3530097fb08f5e8a2271ea736f596ca216729d6e7c5.jpg)



Fig. S12.


Results of the climate change detection and attribution analyses for the ApportionmentEntropy (AE) of river flow in 26 IPCC SREX regions for 1965-2014. (A) Trends of AE inriver flow from multimodel mean of global hydrological models ( $\%$ decade-1), the same asFig. 3B but at global scale. (B) The scaling factor plots from 26 IPCC SREX refer to $10 { - } 9 0 \%$uncertainty ranges from the detection analysis, * indicates a residual consistency test was notpassed $( p < 0 . 1 )$ . Regions with detected signal (lower confidence bound of scaling factor isabove zero (the solid line)) and attributable to ACC (the confidence interval includes one (thedashed line)) are marked with dashes in (A). The ranges of scaling factor are truncated toenhance readability of the plot if confidence intervals exceed the ordinate.

![](images/61dffe3043dcad69601afc2ae605692adea2ecf981f32563cc09366f1af9c30c.jpg)


![](images/d1f5d0f20f2489b3272225f39cd24f2964e713f7c52798dd50e3ff7ba075a7ce.jpg)


![](images/c6bed301ed50925452ec959d459625a95f9476c679a8a505737737dfc2b8acdb.jpg)


# Fig. S13.

River flow seasonality trends represented by apportionment entropy (AE) $\%$ decade-1) over50 years (1965–2014). (A) Degree of regulation $( \% )$ of rivers influenced by reservoirs. (B, C)illustrate the AE trends in the stations influenced by reservoirs (3,914) and those unaffectedby reservoirs (6,150), respectively.

![](images/4b18a0e221d48590be370e0b5a3bd126cbe5d4f06f1be8e343d969d121ce2578.jpg)


![](images/f417288d4a021cf1532c5297ebf04fba948b73b733267f4996a9f971441d2d5c.jpg)



Fig. S14.



Comparison of apportionment entropy (AE) trends from (A) stations and (B) GRUN ( $\%$decade-1). Each grid cell is the median trend for all the stations. Grid cells containing at leastone station were included. Inset is a scatterplot showing the trends from stations and GRUNwith linear regression in a red dashed line. Color shows the relative density of data points.Stations with trends larger than $\pm 6 \%$ are not showed in the scatterplot, which occupied ${ \sim } 4 \%$of 10,120 stations. The purple dashed line at $5 0 \mathrm { { } ^ { \circ } N }$ highlights the boundary of the northernhigh latitudes defined in this study.


![](images/b22dade237b510abc267259c8beec5c554d556fda22748cb6be7dd4c6bb4f228.jpg)



Fig. S15.



Similar to Fig. 3E in the main text, but we replace GRUN with gauged-based observations,and the spatial coverage is restricted to grid cells that contain at least one station in thenorthern high latitudes (above $5 0 \mathrm { ^ { \circ } N }$ ). Correlation coefficient of AE anomalies betweensimulations with and without ACC (corrtemporary(Picontrol, HIST)) or observation-basedreconstructions (corrtemporary(HIST, Station)) across $5 0 ^ { \circ } \mathrm { N } { \cdot } 9 0 ^ { \circ } \mathrm { N }$ . Correlation coefficientbetween the multimodel mean from HIST&HWLU simulations and 216 chunks of Picontrolsimulations with 50-yr segments are shown as an empirical probability density function ingrey. Vertical blue lines mark the $9 5 \%$ and $9 9 \%$ cumulative probability of an assumednormal distribution for the correlation. The inset shows the confidence interval of the scalingfactor plot from the optimal fingerprinting method with $10 { - } 9 0 \%$ uncertainty range.


![](images/8a7d58a8b90218dae3b023f42f0c106c1ca21c4fab5db8214c1e7db93624fd2f.jpg)



Fig. S16.



Trends in Apportionment Entropy (AE) $\%$ decade-1) of precipitation from GPCC in 1965-2014. Black dots indicate a trend significance at 0.05. The purple dashed line at $5 0 \mathrm { { ^ \circ N } }$highlights the boundary of the northern high latitudes defined in this study. Areas of annualprecipitation below $1 0 0 ~ \mathrm { { m m } }$ and Greenland are masked in grey.


![](images/cf7704e422ef4614b7fc9fbd12a58bf7cda3fdcf10db2c7bbff5254d08a6e603.jpg)


![](images/2fb9226b1e1a55493f9b013e37a01080682e17066f10e20a545664541ba0371a.jpg)



Fig. S17.Trends in Apportionment Entropy (AE) $\%$ decade-1 ) of (A) river flow from GRUN and (B)G-RUN ENSEMBLE, reconstructed from observation in 1965-2014. Black dots indicate atrend significance at 0.05. The purple dashed line at $5 0 \mathrm { { } ^ { \circ } N }$ highlights the boundary of thenorthern high latitudes defined in this study. Areas of annual precipitation below 100 mm andGreenland are masked in grey.


![](images/65f4af21af640caf42feae9832ad3c907943a469114a8cda045f03bceee09271.jpg)



Fig. S18.Similar to Fig. 3E in the main text, but with Picontrol simulations restricted inPicontrol&HWLU. Correlation coefficient of AE anomalies between simulations with andwithout ACC (corrtemporary (Picontrol, HIST)) or observation-based reconstructions(corrtemporary(HIST, GRUN)) across $5 0 ^ { \circ } \mathrm { N } { \cdot } 9 0 ^ { \circ } \mathrm { N }$ . Correlation coefficient between themultimodel mean from HIST&HWLU simulations and 108 chunks of Picontrol simulationswith 50-yr segments are shown as an empirical probability density function in grey. Verticalblue lines mark the $9 5 \%$ and $9 9 \%$ cumulative probability of an assumed normal distributionfor the correlation.



Table S1. Spearman’s rank correlation coefficients between the river flow AE withprecipitation AE, soil moisture, snow fraction, and air temperature in the nine hotspots of Fig.S6. * indicates the trends are significant $( p < 0 . 0 5 )$ .


<table><tr><td>Regions</td><td>precipitation AE</td><td>soil moisture</td><td>snow fraction</td><td>air temperature</td></tr><tr><td>N.NA</td><td>-0.57*</td><td>-0.58*</td><td>-0.8*</td><td>0.9*</td></tr><tr><td>N.EU</td><td>0.37*</td><td>0.01</td><td>-0.87*</td><td>0.86*</td></tr><tr><td>W.RU</td><td>-0.78*</td><td>-0.15</td><td>-0.63*</td><td>-0.07</td></tr><tr><td>Alps</td><td>0.06</td><td>-0.77*</td><td>-0.78*</td><td>0.86*</td></tr><tr><td>S.SI</td><td>-0.04</td><td>-0.58*</td><td>-0.67*</td><td>0.55*</td></tr><tr><td>Pacific NW.</td><td>0.65*</td><td>-0.17</td><td>-0.11</td><td>0.32*</td></tr><tr><td>U. Midwest</td><td>-0.2</td><td>0.37*</td><td>-0.89*</td><td>0.83*</td></tr><tr><td>NE.</td><td>0.04</td><td>-0.47*</td><td>-0.55*</td><td>0.7*</td></tr><tr><td>S. BR</td><td>0.93*</td><td>0.27</td><td></td><td>-0.64*</td></tr></table>


Table S2. Ensemble simulations and hydrology models included in our analysis.


<table><tr><td></td><td></td><td colspan="7">climate scenario</td></tr><tr><td></td><td></td><td colspan="4">Pre-industrial control (Picontrl)</td><td colspan="2">Historical (HIST)</td><td>RCP6.0</td></tr><tr><td></td><td>Simulation period social &amp; economy scenarios</td><td>1661-1860</td><td colspan="2">1861-2005</td><td>2006-2099</td><td colspan="2">1861-2005</td><td>2006-2099</td></tr><tr><td>GHM/LSM</td><td>GEM</td><td>1860soc</td><td>histsoc</td><td>2005soc</td><td>2005soc</td><td>histsoc</td><td>2005soc</td><td>2005soc</td></tr><tr><td rowspan="4">CLM45</td><td>GFDL-ESM2M</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td></tr><tr><td>HadGEM2-ES</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td></tr><tr><td>IPSL-CM5A-LR</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td></tr><tr><td>MIROC5</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td></tr><tr><td rowspan="4">H08</td><td>GFDL-ESM2M</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>HadGEM2-ES</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>IPSL-CM5A-LR</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>MIROC5</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td rowspan="4">LPJmL</td><td>GFDL-ESM2M</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>HadGEM2-ES</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>IPSL-CM5A-LR</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>MIROC5</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td rowspan="4">MATSIRO</td><td>GFDL-ESM2M</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>HadGEM2-ES</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>IPSL-CM5A-LR</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>MIROC5</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td rowspan="3">MPI-HM</td><td>GFDL-ESM2M</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>IPSL-CM5A-LR</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>MIROC5</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td rowspan="4">PCR-GLOBWB</td><td>GFDL-ESM2M</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>HadGEM2-ES</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>IPSL-CM5A-LR</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>MIROC5</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td rowspan="4">WaterGAP2</td><td>GFDL-ESM2M</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>HadGEM2-ES</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>IPSL-CM5A-LR</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr><tr><td>MIROC5</td><td>Y</td><td>Y</td><td></td><td>Y</td><td>Y</td><td></td><td>Y</td></tr></table>


Table S3. Monthly streamflow databases included in the analysis during 1970-2019.


<table><tr><td>Database</td><td>Spatial coverage</td><td>Data access information</td></tr><tr><td>Global Runoff Data Centre (GRDC) (48)</td><td>Global</td><td>https://www.bafg.de/GRDC/</td></tr><tr><td>United States Geological Survey water data (USGS)</td><td>USA</td><td>https://waterdata.usgs.gov/nwis</td></tr><tr><td>Canada National Water Data Archive (HYDAT)</td><td>Canada</td><td>https://wateroffice.ec.gc.ca/</td></tr><tr><td>Brazil National Water Agency (ANA)</td><td>Brazil</td><td>http://hidroweb.ana.gov.br/</td></tr><tr><td>African Database of Hydrometric Indices (ADHI) (64)</td><td>Africa</td><td>https://doi.org/10.23708/LXGXQ9</td></tr></table>

# References



1. P. Wu, N. Christidis, P. Stott, Anthropogenic impact on Earth’s hydrological cycle.Nat. Clim. Chang. 3, 807–810 (2013).





2. C. J. Vörösmarty, P. Green, J. Salisbury, R. B. Lammers, Global water resources:Vulnerability from climate change and population growth. Science. 289, 284–288(2000).





3. P. C. D. Milly, K. A. Dunne, A. V. Vecchia, Global pattern of trends in streamflowand water availability in a changing climate. Nature. 438, 347–350 (2005).





4. N. K. Singh, N. B. Basu, The human factor in seasonal streamflows across natural andmanaged watersheds of North America. Nat. Sustain. (2022), doi:10.1038/s41893-022-00848-1.





5. G. Blöschl, J. Hall, A. Viglione, R. A. P. Perdigão, J. Parajka, B. Merz, D. Lun, B.Arheimer, G. T. Aronica, A. Bilibashi, M. Boháč, O. Bonacci, M. Borga, I. Čanjevac,A. Castellarin, G. B. Chirico, P. Claps, N. Frolova, D. Ganora, L. Gorbachova, A. Gül,J. Hannaford, S. Harrigan, M. Kireeva, A. Kiss, T. R. Kjeldsen, S. Kohnová, J. J.Koskela, O. Ledvinka, N. Macdonald, M. Mavrova-Guirguinova, L. Mediero, R. Merz,P. Molnar, A. Montanari, C. Murphy, M. Osuch, V. Ovcharuk, I. Radevski, J. L.Salinas, E. Sauquet, M. Šraj, J. Szolgay, E. Volpi, D. Wilson, K. Zaimi, N. Živković,Changing climate both increases and decreases European river floods. Nature. 573,108–111 (2019).





6. G. Blöschl, J. Hall, J. Parajka, R. A. P. Perdigão, B. Merz, B. Arheimer, G. T. Aronica,A. Bilibashi, O. Bonacci, M. Borga, I. Čanjevac, A. Castellarin, G. B. Chirico, P.Claps, K. Fiala, N. Frolova, L. Gorbachova, A. Gül, J. Hannaford, S. Harrigan, M.Kireeva, A. Kiss, T. R. Kjeldsen, S. Kohnová, J. J. Koskela, O. Ledvinka, N.Macdonald, M. Mavrova-Guirguinova, L. Mediero, R. Merz, P. Molnar, A. Montanari,C. Murphy, M. Osuch, V. Ovcharuk, I. Radevski, M. Rogger, J. L. Salinas, E. Sauquet,M. Šraj, J. Szolgay, A. Viglione, E. Volpi, D. Wilson, K. Zaimi, N. Živković,Changing climate shifts timing of European floods. Science. 357, 588–590 (2017).





7. M. Dynesius, C. Nilsson, Fragmentation and flow regulation of river systems in thenorthern third of the world. Science. 266, 753-762 (1994).





8. M. Palmer, A. Ruhi, Linkages between flow regime, biota, and ecosystem processes:Implications for river restoration. Science. 365 (2019), doi:10.1126/science.aaw2087.





9. T. P. Barnett, J. C. Adam, D. P. Lettenmaier, Potential impacts of a warming climateon water availability in snow-dominated regions. Nature. 438, 303–309 (2005).





10. J. D. Tonkin, D. M. Merritt, J. D. Olden, L. V. Reynolds, D. A. Lytle, Flow regimealteration degrades ecological networks in riparian ecosystems. Nat. Ecol. Evol. 2, 86–93 (2018).





11. S. B. Rood, J. Pan, K. M. Gill, C. G. Franks, G. M. Samuelson, A. Shepherd,Declining summer flows of Rocky Mountain rivers: Changing seasonal hydrology andprobable impacts on floodplain forests. J. Hydrol. 349, 397–410 (2008).





12. H. L. Bateman, D. M. Merritt, Complex riparian habitats predict reptile and amphibiandiversity. Glob. Ecol. Conserv. 22, 1–10 (2020).





13. C. Wasko, R. Nathan, M. C. Peel, Trends in Global Flood and Streamflow TimingBased on Local Water Year. Water Resour. Res. 56, 1–12 (2020).





14. S. Eisner, M. Flörke, A. Chamorro, P. Daggupati, C. Donnelly, J. Huang, Y.Hundecha, H. Koch, A. Kalugin, I. Krylenko, V. Mishra, M. Piniewski, L. Samaniego,O. Seidou, M. Wallner, V. Krysanova, An ensemble analysis of climate changeimpacts on streamflow seasonality across 11 large river basins. Clim. Change. 141,401–417 (2017).





15. K. Marvel, B. I. Cook, C. Bonfils, J. E. Smerdon, A. P. Williams, H. Liu, ProjectedChanges to Hydroclimate Seasonality in the Continental United States. Earth’s Future.9, 1–19 (2021).





16. T. P. Barnett, D. W. Pierce, H. G. Hidalgo, C. Bonfils, B. D. Santer, T. Das, G. Bala,A. W. Wood, T. Nozawa, A. A. Mirin, D. R. Cayan, M. D. Dettinger, Human-inducedchanges in the hydrology of the Western United States. Science. 319, 1080–1083(2008).





17. L. Gudmundsson, S. I. Seneviratne, X. Zhang, Anthropogenic climate change detectedin European renewable freshwater resources. Nat. Clim. Chang. 7, 813–816 (2017).





18. H. X. Do, L. Gudmundsson, M. Leonard, S. Westra, The Global Streamflow Indicesand Metadata Archive (GSIM)-Part 1: The production of a daily streamflow archiveand metadata. Earth Syst. Sci. Data. 10, 765–785 (2018).





19. G. Ghiggi, V. Humphrey, S. I. Seneviratne, L. Gudmundsson, GRUN: An observation-based global gridded runoff dataset from 1902 to 2014. Earth Syst. Sci. Data. 11,1655–1674 (2019).





20. K. Frieler, S. Lange, F. Piontek, C. P. O. Reyer, J. Schewe, L. Warszawski, F. Zhao, L.Chini, S. Denvil, K. Emanuel, T. Geiger, K. Halladay, G. Hurtt, M. Mengel, D.Murakami, S. Ostberg, A. Popp, R. Riva, M. Stevanovic, T. SuzGBRi, J. Volkholz, E.Burke, P. Ciais, K. Ebi, T. D. Eddy, J. Elliott, E. Galbraith, S. N. Gosling, F.Hattermann, T. Hickler, J. Hinkel, C. Hof, V. Huber, J. Jägermeyr, V. Krysanova, R.Marcé, H. Müller Schmied, I. Mouratiadou, D. Pierson, D. P. Tittensor, R. Vautard, M.van Vliet, M. F. Biber, R. A. Betts, B. Leon Bodirsky, D. Deryng, S. Frolking, C. D.Jones, H. K. Lotze, H. Lotze-Campen, R. Sahajpal, K. Thonicke, H. Tian, Y.Yamagata, Assessing the impacts of $1 . 5 ^ { \circ } \mathrm { C }$ global warming - simulation protocol of theInter-Sectoral Impact Model Intercomparison Project (ISIMIP2b). Geosci. Model Dev.10, 4321–4345 (2017).





21. X. Feng, A. Porporato, I. Rodriguez-Iturbe, Changes in rainfall seasonality in thetropics. Nat. Clim. Chang. 3, 811–815 (2013).





22. G. Konapala, A. K. Mishra, Y. Wada, M. E. Mann, Climate change will affect globalwater availability through compounding changes in seasonal precipitation andevaporation. Nat. Commun. 11, 1–10 (2020).





23. A. K. Mishra, M. Özger, V. P. Singh, Association between Uncertainties inMeteorological Variables and Water-Resources Planning for the State of Texas. J.Hydrol. Eng. 16, 984–999 (2011).





24. E. N. Dethier, S. L. Sartain, C. E. Renshaw, F. J. Magilligan, Spatially coherentregional changes in seasonal extreme streamflow events in the United States andCanada since 1950. Sci. Adv. 6 (2020), doi:10.1126/sciadv.aba5939.





25. M. R. Viola, C. R. de Mello, S. C. Chou, S. N. Yanagi, J. L. Gomes, Assessing climatechange impacts on Upper Grande River Basin hydrology, Southeast Brazil. Int. J.Climatol. 35, 1054–1068 (2015).





26. D. Bartiko, D. Y. Oliveira, N. B. Bonumá, P. L. B. Chaffe, Spatial and seasonalpatterns of flood change across Brazil. Hydrol. Sci. J. 64, 1071–1079 (2019).





27. V. Virkki, E. Alanärä, M. Porkka, L. Ahopelto, T. Gleeson, C. Mohan, Globallywidespread and increasing violations of environmental flow envelopes. Hydrol. EarthSyst. Sci. 26, 3315–3336 (2022).





28. X. Lian, S. Piao, L. Z. X. Li, Y. Li, C. Huntingford, P. Ciais, A. Cescatti, I. A.Janssens, J. Peñuelas, W. Buermann, A. Chen, X. Li, R. B. Myneni, X. Wang, Y.Wang, Y. Yang, Z. Zeng, Y. Zhang, T. R. McVicar, Summer soil drying exacerbatedby earlier spring greening of northern vegetation. Sci. Adv. 6, 1–12 (2020).





29. M. Huss, R. Hock, Global-scale hydrological response to future glacier mass loss. Nat.Clim. Chang. 8, 135–140 (2018).





30. E. Gautier, T. Dépret, F. Costard, C. Virmoux, A. Fedorov, D. Grancher, P.Konstantinov, D. Brunstein, Going with the flow: Hydrologic response of middle LenaRiver (Siberia) to the climate variability and change. J. Hydrol. (Amst). 557, 475–488(2018).





31. V. B. P. Chagas, P. L. B. Chaffe, G. Blöschl, Process Controls on Flood Seasonality inBrazil. Geophys. Res. Lett. 49, 1–10 (2022).





32. P. W. Mote, S. Li, D. P. Lettenmaier, M. Xiao, R. Engel, Dramatic declines insnowpack in the western US. NPJ Clim. Atmos. Sci. 1 (2018), doi:10.1038/s41612-018-0012-1.





33. D. Lee, P. J. Ward, P. Block, Identification of symmetric and asymmetric responses inseasonal streamflow globally to ENSO phase. Environ. Res. Lett. 13 (2018),doi:10.1088/1748-9326/aab4ca.





34. R. S. Padrón, L. Gudmundsson, B. Decharme, A. Ducharne, D. M. Lawrence, J. Mao,D. Peano, G. Krinner, H. Kim, S. I. Seneviratne, Observed changes in dry-seasonwater availability attributed to human-induced climate change. Nat. Geosci. 13, 477–481 (2020).





35. L. Grant, I. Vanderkelen, L. Gudmundsson, Z. Tan, M. Perroud, V. M. Stepanenko, A.V Debolskiy, B. Droppers, A. B. G. Janssen, J. Schewe, F. Zhao, I. Vega, M. Golub,D. Pierson, Attribution of global lake systems change to anthropogenic forcing. Nat.Geosci. 14, 849–854 (2021).





36. A. Ribes, S. Planton, L. Terray, Application of regularised optimal fingerprinting toattribution. Part I: Method, properties and idealised analysis. Clim. Dyn. 41, 2817–2836 (2013).





37. B. Arheimer, C. Donnelly, G. Lindström, Regulation of snow-fed rivers affects flowregimes more than climate change. Nat. Commun. 8, 1–8 (2017).





38. L. Gudmundsson, J. Boulange, H. X. Do, S. N. Gosling, M. G. Grillakis, A. G.Koutroulis, M. Leonard, J. Liu, H. M. Schmied, L. Papadimitriou, Y. Pokhrel, S. I.Seneviratne, Y. Satoh, W. Thiery, S. Westra, X. Zhang, F. Zhao, Globally observedtrends in mean and extreme river flow attributed to climate change. Science. 371,1159–1162 (2021).





39. D. Shindell, F. Bréon, W. Collins, J. Fuglestvedt, J. Huang, D. Koch, J. Lamarque, D.Lee, B. Mendoza, T. Nakajima, A. Robock, G. Stephens, T. Takemura, H. Zhang, D.Qin, G. Plattner, M. Tignor, S. Allen, J. Boschung, A. Nauels, Y. Xia, V. Bex, P.Midgley, “Climate Change 2013: The Physical Science Basis” (2013).





40. C. E. Iles, G. C. Hegerl, Systematic change in global patterns of streamflow followingvolcanic eruptions. Nat. Geosci. 8, 838–842 (2015).





41. L. Gudmundsson, J. Kirchner, A. Gädeke, J. Noetzli, B. K. Biskaborn, Attributingobserved permafrost warming in the northern hemisphere to anthropogenic climatechange. Environ. Res. Lett. 17 (2022), doi:10.1088/1748-9326/ac8ec2.





42. W. R. Berghuijs, R. A. Woods, M. Hrachowitz, A precipitation shift from snowtowards rain leads to a decrease in streamflow. Nat. Clim. Chang. 4, 583–586 (2014).





43. M. G. Floriancic, W. R. Berghuijs, P. Molnar, J. W. Kirchner, Seasonality and Driversof Low Flows Across Europe and the United States. Water Resour. Res. 57, 1–17(2021).





44. M. Mudelsee, M. Börngen, G. Tetzlaff, U. Grünewald, No upward trends in theoccurrence of extreme floods in central Europe. Nature. 425, 166–169 (2003).





45. J. D. Hunt, E. Byers, Y. Wada, S. Parkinson, D. E. H. J. Gernaat, S. Langan, D. P. vanVuuren, K. Riahi, Global resource potential of seasonal pumped hydropower storagefor energy and water storage. Nat. Commun. 11, 1–8 (2020).





46. N. L. Poff, J. D. Olden, D. M. Merritt, D. M. Pepin, Homogenization of regional riverdynamics by dams and global biodiversity implications. Proc. Natl. Acad. Sci. U.S.A.104, 5732–5737 (2007).





47. L. Gudmundsson, M. Leonard, H. X. Do, S. Westra, S. I. Seneviratne, ObservedTrends in Global Indicators of Mean and Extreme Streamflow. Geophys. Res. Lett. 46,756–766 (2019).





48. Global Runoff Data Centre, In-situ river discharge data. World MeteorologicalOrganization (2015), (available athttps://www.bafg.de/GRDC/EN/Home/homepage_node.html).





49. G. Ghiggi, V. Humphrey, S. I. Seneviratne, L. Gudmundsson, G-RUN ENSEMBLE:A Multi-Forcing Observation-Based Global Runoff Reanalysis. Water Resour. Res. 57(2021), doi:10.1029/2020WR028787.





50. B. Lehner, K. Verdin, A. Jarvis, New global hydrography derived from spaceborneelevation data. Eos 89, 93–94 (2008).





51. G. Grill, B. Lehner, M. Thieme, B. Geenen, D. Tickner, F. Antonelli, S. Babu, P.Borrelli, L. Cheng, H. Crochetiere, H. E. Macedo, R. Filgueiras, M. Goichot, J.Higgins, Z. Hogan, B. Lip, M. E. Mcclain, J. Meng, M. Mulligan, C. Nilsson, J. D.Olden, J. J. Opperman, P. Petry, C. reidy Liermann, L. Sáenz, S. Salinas-rodríguez, P.Schelle, R. J. P. Schmitt, J. Snider, F. Tan, K. Tockner, P. H. Valdujo, A. vanSoesbergen, C. Zarfl, Mapping the world’s free-flowing rivers. Nature, doi:10.1038/s41586-019-1111-9 (2019).





52. M. Cucchi, G. P. Weedon, A. Amici, N. Bellouin, S. Lange, H. Müller Schmied, H.Hersbach, C. Buontempo, WFDE5: Bias-adjusted ERA5 reanalysis data for impactstudies. Earth Syst. Sci. Data. 12, 2097–2120 (2020).





53. H. Hersbach, B. Bell, P. Berrisford, S. Hirahara, A. Horányi, J. Muñoz-Sabater, J.Nicolas, C. Peubey, R. Radu, D. Schepers, A. Simmons, C. Soci, S. Abdalla, X.Abellan, G. Balsamo, P. Bechtold, G. Biavati, J. Bidlot, M. Bonavita, G. De Chiara, P.Dahlgren, D. Dee, M. Diamantakis, R. Dragani, J. Flemming, R. Forbes, M. Fuentes,A. Geer, L. Haimberger, S. Healy, R. J. Hogan, E. Hólm, M. Janisková, S. Keeley, P.Laloyaux, P. Lopez, C. Lupu, G. Radnoti, P. de Rosnay, I. Rozum, F. Vamborg, S.Villaume, J. N. Thépaut, The ERA5 global reanalysis. Q. J. R. Meteorol. Soc. 1–51(2020).





54. A. Becker, P. Finger, A. Meyer-Christoffer, B. Rudolf, K. Schamm, U. Schneider, M.Ziese, A description of the global land-surface precipitation data products of theGlobal Precipitation Climatology Centre with sample applications including centennial(trend) analysis from 1901-present. Earth Syst. Sci. Data. 5, 71–99 (2013).





55. T. J. Osborn, P. D. Jones, D. H. Lister, C. P. Morice, I. R. Simpson, J. P. Winn, E.Hogan, I. C. Harris, Land Surface Air Temperature Variations Across the GlobeUpdated to 2019: The CRUTEM5 Data Set. J. Geophys. Res. Atmos. 126 (2021),doi:10.1029/2019JD032352.





56. W. T. Pfeffer, A. A. Arendt, A. Bliss, T. Bolch, J. G. Cogley, A. S. Gardner, J. O.Hagen, R. Hock, G. Kaser, C. Kienholz, E. S. Miles, G. Moholdt, N. Mölg, F. Paul, V.Radić, P. Rastner, B. H. Raup, J. Rich, M. J. Sharp, L. M. Andreassen, S. Bajracharya,N. E. Barrand, M. J. Beedle, E. Berthier, R. Bhambri, I. Brown, D. O. Burgess, E. W.Burgess, F. Cawkwell, T. Chinn, L. Copland, N. J. Cullen, B. Davies, H. De Angelis,A. G. Fountain, H. Frey, B. A. Giffen, N. F. Glasser, S. D. Gurney, W. Hagg, D. K.Hall, U. K. Haritashya, G. Hartmann, S. Herreid, I. Howat, H. Jiskoot, T. E.





Khromova, A. Klein, J. Kohler, M. König, D. Kriegel, S. Kutuzov, I. Lavrentiev, R. LeBris, X. Li, W. F. Manley, C. Mayer, B. Menounos, A. Mercer, P. Mool, A. Negrete,G. Nosenko, C. Nuth, A. Osmonov, R. Pettersson, A. Racoviteanu, R. Ranzi, M. A.Sarikaya, C. Schneider, O. Sigurdsson, P. Sirguey, C. R. Stokes, R. Wheate, G. J.Wolken, L. Z. Wu, F. R. Wyatt, The randolph glacier inventory: A globally completeinventory of glaciers. J. Glaciol. 60, 537–552 (2014).





57. J. Brown, O. Ferrians, J. A. Heginbottom, E. Melnikov, Circum-Arctic Map ofPermafrost and Ground-Ice Conditions, Version 2, National Snow and Ice Data Center(2002).





58. C. E. Telteu, H. Müller Schmied, W. Thiery, G. Leng, P. Burek, X. Liu, J. E. S.Boulange, L. S. Andersen, M. Grillakis, S. N. Gosling, Y. Satoh, O. Rakovec, T.Stacke, J. Chang, N. Wanders, H. L. Shah, T. Trautmann, G. Mao, N. Hanasaki, A.Koutroulis, Y. Pokhrel, L. Samaniego, Y. Wada, V. Mishra, J. Liu, P. Döll, F. Zhao,A. Gädeke, S. S. Rabin, F. Herz, Understanding each other’s models An introductionand a standard representation of 16 global water models to support intercomparison,improvement, and communication. Geosci. Model Dev. 14, 3843–3878 (2021).





59. P. K. Sen, Estimates of the Regression Coefficient Based on Kendall’s Tau. J. Am.Stat. Assoc. 63, 1379–1389 (1968).





60. K. Stahl, L. M. Tallaksen, J. Hannaford, H. A. J. Van Lanen, Filling the white space onmaps of European runoff trends: estimates from a multi-model ensemble. Hydrol.Earth Syst. Sci. 16, 2035–2047 (2012).





61. X. Zhou, X. Huang, H. Zhao, K. Ma, Development of a revised method for indicatorsof hydrologic alteration for analyzing the cumulative impacts of cascading reservoirson flow regime. Hydrol. Earth Syst. Sci. 24, 4091–4107 (2020).





62. K. Timpe, D. Kaplan, The changing hydrology of a dammed Amazon. Sci. Adv. 3, 1–14 (2017).





63. M. R. Allen, P. A. Stott, Estimating signal amplitudes in optimal fingerprinting, part I:Theory. Clim. Dyn. 21, 477–491 (2003).





64. Y. Tramblay, N. Rouché, J.-E. Paturel, G. Mahé, J.-F. Boyer, E. Amoussou, A.Bodian, H. Dacosta, H. Dakhlaoui, A. Dezetter, D. Hughes, L. Hanich, C. Peugeot, R.Tshimanga, P. Lachassagne, The African Database of Hydrometric Indices (ADHI).Earth Syst. Sci. Data. 9, 1–21 (2020).

