import pandas as pd
import praw
import time

def scrape_reddit(subreddit_names, keywords, limit = 100):
    try:
        # Initialize PRAW
        user_agent = "webscrapping reddit by /u/leonlu178"
        reddit = praw.Reddit(
            client_id = "UcxsCcRemuvYXyw4STDmYg",
            client_secret = "9sJtjuiRkUmR23euHBhwbG-sMdgzXg",
            user_agent = user_agent
        )

        data = []
        # condition = False
        # start_time = time.time()
        for subreddit_name in subreddit_names:
            # Choose subreddit
            subreddit = reddit.subreddit(subreddit_name)
            
            # Iterate through keywords
            for keyword in keywords:
                # Iterate through submissions
                for submission in subreddit.search(keyword, sort='relevance', time_filter='year', limit=limit):
                    submission_data = {
                        "subreddit": subreddit_name,
                        "title": submission.title,
                        "score": submission.score,
                        "id": submission.id,
                        "url": submission.url,
                        "text": submission.selftext,
                        "comments": []
                    }

                    submission.comments.replace_more(limit=None)
                    for comment in submission.comments:
                        comment_data = {
                            "body": comment.body,
                            "score": comment.score
                        }
                        submission_data['comments'].append(comment_data)

                    data.append(submission_data)
                    
                    # Introduce a delay between API calls
                    time.sleep(1)

            #         if time.time() - start_time >= 90:
            #             condition = True
            #             break
            #     if condition:
            #         break
            # if condition:
            #     break

        # Convert to pandas
        df = pd.DataFrame(data)
        df.to_json("reddit_data.json", orient="records")
        df.to_csv("reddit_data.csv", index=False)
        return df

    except Exception as e:
        print(f"An error occured: {e}")
        return None


