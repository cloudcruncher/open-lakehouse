select * from {{ ref('card_auth_daily') }} where approved + declined <> auths
