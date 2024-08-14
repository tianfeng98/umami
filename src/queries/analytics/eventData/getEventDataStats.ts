import prisma from 'lib/prisma';
import clickhouse from 'lib/clickhouse';
import { CLICKHOUSE, PRISMA, runQuery } from 'lib/db';
import { QueryFilters } from 'lib/types';

export async function getEventDataStats(
  ...args: [websiteId: string, filters: QueryFilters]
): Promise<{
  events: number;
  fields: number;
  records: number;
}> {
  return runQuery(
    {
      [PRISMA]: () => relationalQuery(...args),
      [CLICKHOUSE]: () => clickhouseQuery(...args),
    },
    'getEventDataStats',
  ).then(results => results[0]);
}

async function relationalQuery(websiteId: string, filters: QueryFilters) {
  const { rawQuery, parseFilters } = prisma;
  const { filterQuery, params } = await parseFilters(websiteId, filters);

  return rawQuery(
    `
    select 
      count(distinct t.website_event_id) as "events",
      count(distinct t.data_key) as "fields",
      sum(t.total) as "records"
    from (
      select
        website_event_id,
        data_key,
        count(*) as "total"
      from event_data
      where website_id = {{websiteId::uuid}}
        and created_at between {{startDate}} and {{endDate}}
      ${filterQuery}
      group by website_event_id, data_key
      ) as t
    `,
    params,
  );
}

async function clickhouseQuery(
  websiteId: string,
  filters: QueryFilters,
): Promise<{ events: number; fields: number; records: number }[]> {
  const { rawQuery, parseFilters } = clickhouse;
  const { filterQuery, params } = await parseFilters(websiteId, filters);

  return rawQuery(
    `
    select 
      count(distinct t.event_id) as "events",
      count(distinct t.data_key) as "fields",
      sum(t.total) as "records"
    from (
      select
        event_id,
        data_key,
        count(*) as "total"
      from event_data
      where website_id = {websiteId:UUID}
        and created_at between {startDate:DateTime64} and {endDate:DateTime64}
      ${filterQuery}
      group by event_id, data_key
      ) as t
    `,
    params,
  ).then(result => {
    return Object.values(result).map((a: any) => {
      return {
        events: Number(a.events),
        fields: Number(a.fields),
        records: Number(a.records),
      };
    });
  });
}
