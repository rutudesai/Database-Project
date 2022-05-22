EXPLAIN
SELECT tmp.retweet_count,u.followers_count,*
FROM tweet t 
INNER JOIN "user" u ON t.user_id = u.id
LEFT JOIN (SELECT parent_tweet_id, COUNT(1) as retweet_count
            FROM retweetmap rt 
            GROUP BY parent_tweet_id) tmp ON t.id = tmp.parent_tweet_id
WHERE t.id in (1254022772877131777,1254022774521081856,
			   1254024716899291137,1254024115859517441,1254034810231717889)
ORDER BY 
COALESCE(tmp.retweet_count,0) desc,
u.followers_count desc;

CREATE OR REPLACE FUNCTION tweet_sort(BIGINT[]) RETURNS SETOF BIGINT AS
$BODY$
DECLARE
        in_clause ALIAS FOR $1;
        clause  TEXT;
        rec     RECORD;
BEGIN

		FOR rec IN SELECT t.id
					FROM tweet t 
					INNER JOIN "user" u ON t.user_id = u.id
					LEFT JOIN (SELECT parent_tweet_id, COUNT(1) as retweet_count
								FROM retweetmap rt 
								GROUP BY parent_tweet_id) tmp ON t.id = tmp.parent_tweet_id
					WHERE t.id = ANY(in_clause)
					ORDER BY 
					COALESCE(tmp.retweet_count,0) desc,
					u.followers_count desc
		LOOP
                RETURN NEXT rec;
        END LOOP;
        -- final return
        RETURN ;
END
$BODY$ language plpgsql;


create or replace procedure add_tweet(
	--tweet
	tweet_id bigint,
	tweet_text TEXT, 
	tweet_quote_count bigint,
	tweet_reply_count bigint,
	tweet_retweet_count bigint,
	tweet_fav_count bigint,
	tweet_is_retweet boolean,
	tweet_created_at bigint,
	--user
	tweet_user_id bigint,
	user_name TEXT,
	user_screen_name TEXT,
	user_location TEXT,
	user_follow_count bigint,
	user_friends_count bigint,
	user_listed_count bigint,
	user_fav_count bigint,
	user_statuses_count bigint,
	user_created_at bigint,
	--rwtweet
	original_tweet_id bigint
)
language plpgsql    
as $$
DECLARE 
	user_modified_at bigint;
begin
	--Create or Update the user

	SELECT modified_at INTO user_modified_at
	FROM public."user" 
	WHERE id = tweet_user_id limit 1;
	
	IF user_modified_at IS NULL THEN
		INSERT INTO public."user"("id", "name", screen_name, "location", 
								  followers_count, friends_count, listed_count, 
								  fav_count, statuses_count, 
								  created_at, modified_at)
		VALUES (tweet_user_id, user_name, user_screen_name, user_location, 
				user_follow_count, user_friends_count, user_listed_count, 
				user_fav_count, user_statuses_count, 
				user_created_at, tweet_created_at)
		ON CONFLICT ("id") 
		DO NOTHING;
	ELSIF tweet_created_at > user_modified_at THEN
		UPDATE public."user"
		SET 
		followers_count = user_follow_count,
		friends_count = user_friends_count,
		listed_count = user_listed_count,
		fav_count = user_fav_count,
		statuses_count = user_statuses_count,
		modified_at = tweet_created_at
		WHERE "id" = tweet_user_id;
	END IF;
	
	--INSERT TWEET
	INSERT INTO public.tweet("id", "text", quote_count, reply_count, 
							 retweet_count, fav_count, is_retweet, 
							 user_id, created_at, modified_at)
	VALUES (tweet_id, tweet_text, tweet_quote_count, tweet_reply_count, 
			tweet_retweet_count, tweet_fav_count, tweet_is_retweet, 
			tweet_user_id, tweet_created_at, tweet_created_at)
	ON CONFLICT ("id") 
	DO NOTHING;
	
	--INSERT RETWEET
	IF original_tweet_id IS NOT NULL THEN
		INSERT INTO public.retweetmap(retweet_id, parent_tweet_id, 
									  created_at, modified_at)
		VALUES (tweet_id, original_tweet_id, tweet_created_at, tweet_created_at)
		ON CONFLICT (retweet_id, parent_tweet_id) 
		DO NOTHING;
	END IF;
	
--     commit;
-- 	RETURN 0;
end;$$;