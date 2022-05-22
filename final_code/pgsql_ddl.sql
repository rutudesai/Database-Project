SELECT 'CREATE DATABASE twitter'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'twitter')\gexec

USE twitter;

CREATE TABLE IF NOT EXISTS "user" (
    id BIGINT PRIMARY KEY,
    "name" TEXT NOT NULL,
    screen_name TEXT NOT NULL,
    "location" TEXT,
    followers_count BIGINT DEFAULT 0,
    friends_count BIGINT DEFAULT 0,
    listed_count BIGINT DEFAULT 0,
    fav_count BIGINT DEFAULT 0,
    statuses_count BIGINT DEFAULT 0,
    created_at BIGINT NOT NULL,
    modified_at BIGINT NOT NULL
);


CREATE TABLE IF NOT EXISTS tweet (
    id BIGINT PRIMARY KEY,
    "text" TEXT NOT NULL,
    quote_count BIGINT DEFAULT 0,
    reply_count BIGINT DEFAULT 0,
    retweet_count BIGINT DEFAULT 0,
    fav_count BIGINT DEFAULT 0,  
    is_retweet boolean DEFAULT false,
    user_id BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    modified_at BIGINT NOT NULL DEFAULT extract(epoch from now()),
    CONSTRAINT fk_user FOREIGN KEY(user_id) REFERENCES "user"(id)
);


CREATE TABLE IF NOT EXISTS retweetmap (
    retweet_id BIGINT,
    parent_tweet_id BIGINT,
    created_at BIGINT NOT NULL,
    modified_at BIGINT NOT NULL DEFAULT extract(epoch from now()),
    PRIMARY KEY(retweet_id, parent_tweet_id),
    CONSTRAINT fk_retweetid FOREIGN KEY(retweet_id) REFERENCES tweet(id)
);

-- Index: user_followers_count_friends_count_idx

-- DROP INDEX IF EXISTS public.user_followers_count_friends_count_idx;

CREATE INDEX IF NOT EXISTS user_followers_count_friends_count_idx
    ON public."user" USING btree
    (followers_count DESC NULLS FIRST, friends_count DESC NULLS FIRST)
    TABLESPACE pg_default;
