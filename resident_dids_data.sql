
--Week, Modlaity, Added, removed, waitinglist size.
 --86 weeks from 07/04/2024 to 23/11/2025 with missing weeks for some
 --select weekendingdate from #diagnostics where descriptor='Barium Enema'group by weekendingdate order by 1

drop table	if exists #latestweek;
drop table	if exists #tempremovals;
drop table	if exists #adds;

select		max(WeekEndingDate) as Latest 
into		#latestweek 
from		[EAT_Reporting_BSOL].[Development].[BSOL0057_Data_Resident]

declare		@end date
set			@end=(select* from #latestweek)

/**************************************************************************************************
Dates table for every week needed... 
**************************************************************************************************/
drop table	if exists #dates;
select		WeekCommencingDate
,			WeekEndingDate 
into		#dates
from		[EAT_Reporting_BSOL].[Reporting].[BSOLBI_0057_dim_Dates]
where		WeekEndingDate>='01-Apr-2024'
and			WeekEndingDate<=@end
group by	WeekEndingDate
,			WeekCommencingDate

--select * from #dates order by 1

/**************************************************************************************************
Diagnostics modelling table creation and insertion... 
**************************************************************************************************/
drop table if exists #diagnostics;
create table #diagnostics(
			row_num int
,			Descriptor			varchar(100)
,			WeekCommencingDate	date
,			WeekEndingDate		date
,			OpeningBalance		int
,			Additions			int
,			Removals			int
,			WL_Total			int
,			ClosingBalance		int
)

insert into #diagnostics(
			row_num 
,			Descriptor
,			WeekCommencingDate
,			WeekEndingDate		
)

--this puts a row for every week ending (and commencing) date into the table.... 
(
Select		ROW_NUMBER() OVER (PARTITION BY descriptor order by Descriptor, dt.weekendingdate) AS row_num
,			T2.Descriptor
,			dt.WeekCommencingDate
,			dt.WeekEndingDate	
from		#dates dt
cross join	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefDiagMod] t2
where		t2.Descriptor<>'999 Un-Mapped [Diagnostic_Modality]'
group by 	T2.Descriptor
,			dt.WeekCommencingDate
,			dt.WeekEndingDate	
)


update		#diagnostics
set			OpeningBalance=0
,			Additions=0
,			removals=0
,			WL_Total=0
,			ClosingBalance=0


update		t1
set			t1.additions=tmp.Additions
,			t1.WL_Total=tmp.WL_Total
,			t1.OpeningBalance=tmp.OpeningBalance
from		#diagnostics t1
left join (

Select		Descriptor
,			WeekEndingDate
,			cast(null as int) as OpeningBalance
,			cast(null as int) as Additions
,			cast(NULL as int)  as Removals
,			sum(Counter) as WL_Total 
,			cast(null as int) as ClosingBalance

from		[EAT_Reporting_BSOL].[DEVELOPMENT].[BSOL0057_Data_Resident] T1
LEFT JOIN	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefDiagMod] T2
ON			T1.Modality = t2.diagnostic_modality 
left join	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefWListType] t3
on			t1.waitinglisttype=t3.waitinglisttype
Where		WeekEndingDate >= '2024-Apr-01'
and			Descriptor is not null
and			Descriptor not in ('999 Un-Mapped [Diagnostic_Modality]')
--and			WaitingListDescription not in ('Not Mapped','Direct access service other')
--and			NHSNumber='27117714'
group by	Descriptor
,			WeekEndingDate
)tmp
on			t1.Descriptor=tmp.Descriptor
where		t1.WeekEndingDate=tmp.WeekEndingDate


-- Extra thought as additions seem low:

;with cte as
(
Select		T1.*, T2.Descriptor, t3.WaitingListDescription
,			ROW_NUMBER() OVER (PARTITION BY NHSNumber, PatPathwayID order by WeekEndingDate) as rn
 

from		[EAT_Reporting_BSOL].[DEVELOPMENT].[BSOL0057_Data_Resident] T1
LEFT JOIN	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefDiagMod] T2
ON			T1.Modality = t2.diagnostic_modality 
left join	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefWListType] t3
on			t1.waitinglisttype=t3.waitinglisttype
Where		WeekEndingDate >= '2024-Apr-01'
and			Descriptor is not null
and			Descriptor not in ('999 Un-Mapped [Diagnostic_Modality]')
--and			WaitingListDescription not in ('Not Mapped','Direct access service other')
--and			NHSNumber='27117714'
)

Select Descriptor,
WeekEndingDate,
count(*) as Additions
into #adds
from cte
Where rn = 1
group by Descriptor,
WeekEndingDate


Update t1
set Additions = t2.Additions
FROM #diagnostics t1 left join #adds t2 ON t1.Descriptor = t2.Descriptor and t1.WeekEndingDate = t2.WeekEndingDate



;with cte as
(
Select		T1.*, T2.Descriptor, t3.WaitingListDescription
,			ROW_NUMBER() OVER (PARTITION BY NHSNumber, PatPathwayID order by WeekEndingDate desc) as rn

from		[EAT_Reporting_BSOL].[DEVELOPMENT].[BSOL0057_Data_Resident] T1
LEFT JOIN	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefDiagMod] T2
ON			T1.Modality = t2.diagnostic_modality 
left join	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefWListType] t3
on			t1.waitinglisttype=t3.waitinglisttype
Where		WeekEndingDate >= '2024-Apr-01'
and			Descriptor is not null
and			Descriptor not in ('999 Un-Mapped [Diagnostic_Modality]')
--and			WaitingListDescription not in ('Not Mapped','Direct access service other')
--and			NHSNumber='27117714'
)

Select Descriptor,
WeekEndingDate,
count(*) as Removals
into #removals
from cte
Where rn = 1
group by Descriptor,
WeekEndingDate


Update t1
set Removals = t2.Removals
FROM #diagnostics t1 left join #removals t2 ON t1.Descriptor = t2.Descriptor and t1.WeekEndingDate = t2.WeekEndingDate





--put in the previous week's WL size into the [Removals column for next part]
update		t1
set			t1.OpeningBalance=t2.WL_Total
from		#diagnostics t1
left join	#diagnostics t2
on			t1.Descriptor=t2.Descriptor
where		t1.row_num=t2.row_num+1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/******************************************************************************************
Now let's get the removals sorted...
******************************************************************************************/
drop table if exists #tempremovals;
select		ProviderCode
,			ClockStartDate
,			max(WeekEndingDate) as LatestWeekEndingDate
,			cast(null as date) as WeekendingRemovalDate
,			t2.Descriptor
,			t1.PatPathwayID
,			cast(0 as smallint) as REMOVAL
into		#tempremovals
from		[EAT_Reporting_BSOL].[Development].[BSOL0057_Data_Resident] t1
LEFT JOIN	[EAT_Reporting_BSOL].[Development].[BSOL0057_RefDiagMod] T2
ON			t1.Modality = t2.diagnostic_modality
where		1=1
--and		NHSNumber='27117714'
and			datename(weekday,WeekEndingDate)='Sunday'
group by	ProviderCode
,			ClockStartDate
,			t2.Descriptor
,			t1.PatPathwayID
order by	t2.Descriptor
,			ClockStartDate
--select * from #tempremovals
--select * from #diagnostics

--adds 7 days on 
update		#tempremovals
set			WeekendingRemovalDate=dateadd(ww,+1,LatestWeekendingDate)
,			REMOVAL=1

--removes un-necessary columns now so the final table can be aggregated to Modality-level data
alter table #tempremovals
drop column LatestWeekEndingDate

alter table #tempremovals
drop column Providercode

alter table #tempremovals
drop column clockstartdate

alter table #tempremovals
drop column PatPathwayID

delete from #tempremovals
where		Descriptor='999 Un-Mapped [Diagnostic_Modality]' --gets rid of the few, for the many!

delete from #tempremovals 
where		WeekendingRemovalDate>=@end --removes the rolling latest week ending date as it will chop a lot off!

--Handling the nulls now.... 
update		#diagnostics
set			OpeningBalance=0
where		OpeningBalance is null

update		#diagnostics
set			Additions=0
where		Additions is null

update		#diagnostics
set			Removals=0
where		Removals is null

update		#diagnostics
set			ClosingBalance=0
where		ClosingBalance is null


/******************************************************************************************
Put in the Removals to the #tempdiagnostics table now....
******************************************************************************************/

update		t1
set			t1.Removals=t2.Removal
from		#diagnostics t1

left join	(	select		weekendingremovaldate
,							descriptor
,							sum(removal) as removal 
				from		#tempremovals 
				group by	weekendingremovaldate
,							descriptor
			) t2
on			t1.Descriptor=t2.Descriptor
where		t1.WeekEndingDate=t2.WeekendingRemovalDate
and			t2.Descriptor is not null


update		#diagnostics
set			ClosingBalance=(OpeningBalance+Additions)-Removals


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--final numbers.... 
select		* 
from		#diagnostics
where		1=1
--and			Descriptor='Barium Enema' --used this Chris as it's easy numbers to mathematically work out - the column to use is the REVISED WL size...
order		by 2,3,4

