select
    t2.title, 
    t2.description, 
    cast(t2."publishedAt" as timestamp) as "publishedAt", 
    cast(t2."viewCount" as bigint) as "viewCount", 
    cast(t2."likeCount" as bigint) as "likeCount", 
    cast(t2."commentCount" as integer) as "commentCount", 
    t2.video_id,

    cast(t1."rank" as integer) as channel_rank,
    t1.channel_name,
    t1.channel_link,
    t1.channel_id,
    cast(t1.uploads as bigint) as channel_uploads, 
    cast(t1.subs as bigint) as channel_subs,
    cast(t1."views" as bigint) as channel_totalviews,
    t1.category as channel_categ
    
from {{ source('landing_sources', 'landing_channels_ranking') }} t1

inner join {{ source('landing_sources', 'landing_video_stats') }} t2
    on t1.channel_id = t2.channel_id
