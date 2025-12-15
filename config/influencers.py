"""
Categorical User Tags for Source Scoring
Trusted sources get higher weights in the composite signal
"""

CATEGORICAL_TAGS = {
    # Tier 1: Verified News Sources (Weight: 1.0)
    "verified_news": {
        "cnbc", "cnbctv18", "etnow", "zeebusiness", "moneycontrol",
        "bloombergquint", "ndtvprofit","economictimes", "livemint", "financialexpress", "thehindubusinessline"
    },
    
    # Tier 2: Market Influencers (Weight: 0.7)
    "influencer": {
        "rachana_ranade","narendramodi_in"
    }
}

# Tier 3: Random users get default weight of 0.3

