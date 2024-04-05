
from dash import Dash, html, dcc, Input, Output  # pip install dash
#import dash_ag_grid as dag                       # pip install dash-ag-grid
import dash_bootstrap_components as dbc          # pip install dash-bootstrap-components
import numpy as np                               # pip install numpy
import pandas as pd                              # pip install pandas
import seaborn as sns                            # pip install seaborn

import matplotlib                                # pip install matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import base64
from io import BytesIO

import json

import os
from google.cloud import bigquery                # pip install --upgrade google-cloud-bigquery
from google.oauth2 import service_account

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "IS3107 Keys.json"
# client = bigquery.Client()
cwd = os.getcwd()
print(cwd)
credentials = service_account.Credentials.from_service_account_file('/home/is3107group10/dashboard/IS3107 Keys.json')
client = bigquery.Client(credentials=credentials)
import db_dtypes                                 # pip install db-dtypes

from wordcloud import WordCloud, STOPWORDS       # pip install wordcloud
import PIL.Image


def return_query_results():
	sql_query = """
	WITH Keywords AS (
	SELECT Keyword
	FROM UNNEST(["government", "ministry", "budget", "GST", "CPF", "education", "healthcare", "transport", "housing", "PAP", "elections", "ministers"]) AS keyword
	)

	SELECT
	(AVG(sentiments.polarity) + 1) / 2 * 5 AS Avg_Sentiment
	FROM
	`is3107-group-10.Dataset.Reddit Data with sentiment scores`,
	UNNEST(sentiments) AS sentiments
	"""

	gauge_graph_result = client.query(sql_query).to_dataframe()

	sql_query = """
	WITH Keywords AS (
	SELECT Keyword
	FROM UNNEST(["government", "ministry", "budget", "GST", "CPF", "education", "healthcare", "transport", "housing", "PAP", "elections", "ministers"]) AS keyword
	)

	SELECT
	CASE
		WHEN sentiments.polarity > 0.1 THEN 'Positive'
		WHEN sentiments.polarity < -0.1 THEN 'Negative'
		ELSE 'Neutral'
	END AS Sentiment,
	COUNT(*) AS Count
	FROM
	`is3107-group-10.Dataset.Reddit Data with sentiment scores`,
	UNNEST(sentiments) AS sentiments
	GROUP BY
	CASE
		WHEN sentiments.polarity > 0.1 THEN 'Positive'
		WHEN sentiments.polarity < -0.1 THEN 'Negative'
		ELSE 'Neutral'
	END
	"""

	donut_graph_result = client.query(sql_query).to_dataframe()

	sql_query = """
	SELECT
	COUNT(*) AS Count
	FROM
	`is3107-group-10.Dataset.Reddit Data with sentiment scores`
	"""

	total_num_of_comments_result = client.query(sql_query).to_dataframe()

	sql_query = """
	SELECT
	COUNT(*) AS Count
	FROM
	`is3107-group-10.Dataset.Straitstimes Data with sentiment scores`
	"""

	total_num_of_articles_result = client.query(sql_query).to_dataframe()

	sql_query = """
	WITH Keywords AS (
	SELECT Keyword
	FROM UNNEST(["government", "ministry", "budget", "GST", "CPF", "education", "healthcare", "transport", "housing", "PAP", "elections", "ministers"]) AS keyword
	)

	SELECT
	Keyword,
	CASE
		WHEN sentiments.polarity > 0.1 THEN 'Positive'
		WHEN sentiments.polarity < -0.1 THEN 'Negative'
		ELSE 'Neutral'
	END AS Sentiment,
	COUNT(*) AS Count
	FROM
	`is3107-group-10.Dataset.Reddit Data with sentiment scores`,
	UNNEST(sentiments) AS sentiments
	JOIN
	Keywords
	ON
	LOWER(comments_body) LIKE '%' || keyword || '%'
	GROUP BY
	Keyword,
	CASE
		WHEN sentiments.polarity > 0.1 THEN 'Positive'
		WHEN sentiments.polarity < -0.1 THEN 'Negative'
		ELSE 'Neutral'
	END
	ORDER BY Keyword, Sentiment
	"""

	sentiment_by_category_result = client.query(sql_query).to_dataframe()

	sql_query = """
	WITH Keywords AS (
	SELECT Keyword
	FROM UNNEST(["government", "ministry", "budget", "GST", "CPF", "education", "healthcare", "transport", "housing", "PAP", "elections", "ministers"]) AS keyword
	)

	SELECT
	Keyword,
	(sentiments.polarity + 1) / 2 * 5 AS Sentiments
	FROM
	`is3107-group-10.Dataset.Reddit Data with sentiment scores`,
	UNNEST(sentiments) AS sentiments
	JOIN
	Keywords
	ON
	LOWER(comments_body) LIKE '%' || keyword || '%'
	ORDER BY Keyword
	"""

	sentiments_boxplot_result = client.query(sql_query).to_dataframe()

	sql_query = """
	WITH Keywords AS (
	SELECT Keyword
	FROM UNNEST(["government", "ministry", "budget", "GST", "CPF", "education", "healthcare", "transport", "housing", "PAP", "elections", "ministers"]) AS keyword
	)

	SELECT
	Keyword,
	sentiments.subjectivity * 5 AS Subjectivity
	FROM
	`is3107-group-10.Dataset.Straitstimes Data with sentiment scores`
	JOIN
	Keywords
	ON
	LOWER(title) LIKE '%' || keyword || '%'
	ORDER BY Keyword
	"""

	subjectivity_boxplot_result = client.query(sql_query).to_dataframe()

	sql_query = """
	SELECT comments_body
	FROM `is3107-group-10.Dataset.Reddit Data with sentiment scores`
	"""

	reddit_wordcloud_result = client.query(sql_query).to_dataframe()

	sql_query = """
	SELECT title, text
	FROM `is3107-group-10.Dataset.Straitstimes Data with sentiment scores`
	"""
	import pandas as pd

	news_wordcloud_result = client.query(sql_query).to_dataframe()

	sql_query = """
	SELECT
	sentiments.polarity AS sentiments
	FROM
	`is3107-group-10.Dataset.Reddit Data with sentiment scores`,
	UNNEST(sentiments) AS sentiments
	"""

	sentiment_density_plot_result = client.query(sql_query).to_dataframe()

	sql_query = """
	SELECT
	sentiments.subjectivity AS sentiments
	FROM
	`is3107-group-10.Dataset.Straitstimes Data with sentiment scores`
	"""

	subjectivity_density_plot_result = client.query(sql_query).to_dataframe()

	return {
		"gauge_graph_result": gauge_graph_result,
		"donut_graph_result": donut_graph_result,
		"total_num_of_comments_result": total_num_of_comments_result,
		"total_num_of_articles_result": total_num_of_articles_result,
		"sentiment_by_category_result": sentiment_by_category_result,
		"sentiments_boxplot_result": sentiments_boxplot_result,
		"subjectivity_boxplot_result": subjectivity_boxplot_result,
		"reddit_wordcloud_result": reddit_wordcloud_result,
		"news_wordcloud_result": news_wordcloud_result,
		"sentiment_density_plot_result": sentiment_density_plot_result,
		"subjectivity_density_plot_result": subjectivity_density_plot_result
	}


query_results = return_query_results()

def save_to_buffer(fig):
	buf = BytesIO()
	fig.savefig(buf, format="png")
    # Embed the result in the html output.
	fig_data = base64.b64encode(buf.getbuffer()).decode("ascii")
	return f'data:image/png;base64,{fig_data}'


def gauge_graph_as_html():
	mean_sentiment = query_results["gauge_graph_result"]['Avg_Sentiment'][0]
	colors = ['#4dab6d', '#72c66e', '#f6ee54', '#f36d54', '#ee4d55']
	values = [5, 4, 3, 2, 1, 0]
	fig = plt.figure(figsize=(3.5,3.5))
	ax = fig.add_subplot(projection="polar")
	ax.bar(x=[0, 0.628, 1.256, 1.884, 2.512], width=0.628, height=0.5, bottom=2,
		linewidth=3, edgecolor="white",
		color=colors, align="edge")

	# plt.annotate("Positive", xy=(0.25,2.05), rotation=-70, color="white", fontweight="bold");
	# plt.annotate("Slightly positive", xy=(1,1.77), rotation=-35, color="white", fontweight="bold");
	# plt.annotate("Neutral", xy=(1.66,2.22), color="white", fontweight="bold");
	# plt.annotate("Slightly negative", xy=(2.42,2.27), rotation=37, color="white", fontweight="bold");
	# plt.annotate("Negative", xy=(2.92,2.27), rotation=75, color="white", fontweight="bold");

	for loc, val in zip([0, 0.628, 1.256, 1.884, 2.512, 3.14], values):
		plt.annotate(val, xy=(loc, 2.5), ha="right" if val<=2 else "left")


	plt.annotate(round(mean_sentiment,1), xytext=(0,0), xy=((5-mean_sentiment)*0.628, 2.0),
				arrowprops=dict(arrowstyle="wedge, tail_width=0.5", color="black", shrinkA=0),
				bbox=dict(boxstyle="circle", facecolor="black", linewidth=2.0, ),
				fontsize=15, color="white", ha="center"
				);

	ax.set_axis_off()
	ax.set_thetamin(0)
	ax.set_thetamax(180)

	return_val = save_to_buffer(fig)
	plt.close()
	return return_val

def donut_graph_as_html():
	colors = ['#4dab6d', '#f6ee54', '#ee4d55']
	fig, ax = plt.subplots(figsize=(3.5,3.5))

	ax.pie(list(query_results["donut_graph_result"]['Count']),
		wedgeprops={'width':0.25},
		startangle=90,
		colors=colors)

	label = plt.annotate(
		"Sentiment \nBreakdown",
		xy=(0, 0),
		fontsize=20,
		verticalalignment="center",
		horizontalalignment="center"
	)

	return_val = save_to_buffer(fig)
	plt.close()
	return return_val

def sentiment_by_category_as_html(type):
	category_sentiment_breakdown_df = query_results["sentiment_by_category_result"].pivot(index='Keyword', columns='Sentiment', values='Count')
	groups = list(category_sentiment_breakdown_df['Negative'].keys())[::-1]
	values1 = category_sentiment_breakdown_df['Negative'][::-1]
	values2 = category_sentiment_breakdown_df['Neutral'][::-1]
	values3 = category_sentiment_breakdown_df['Positive'][::-1]

	if type == "Absolute":
		fig, ax = plt.subplots(figsize=(9.20, 5.28))
		ax.barh(groups, values1, color = '#ee4d55', label = 'Negative')
		ax.barh(groups, values2, left = values1, color='#f6ee54', label = 'Neutral')
		ax.barh(groups, values3, left = np.add(values1, values2), color='#4dab6d', label = 'Positive')
		ax.legend()
		plt.tight_layout()
		ax.grid(axis = "x", color="black", alpha = 0.25)

	elif type == "Scatterplot":
		fig = plt.figure(figsize=(6,5.46))
		ax = fig.add_subplot(111)

		plt.scatter(values3, values1, color="purple", s=200)
		plt.xlabel("Positive", size=12)
		plt.ylabel("Negative", size=12)
		for i, txt in enumerate(groups):
			ax.annotate(txt, (values3[i], values1[i]), xytext=(values3[i]+18.8, values1[i]+2))
		plt.subplots_adjust(right=0.9)

	elif type == "Relative":
		total_arr = []
		for i in range(len(values1)):
			total_arr.append(values1[i] + values2[i] + values3[i])
		normalized_values1 = [x/total_arr[i] for i, x in enumerate(values1)]
		normalized_values2 = [x/total_arr[i] for i, x in enumerate(values2)]
		normalized_values3 = [x/total_arr[i] for i, x in enumerate(values3)]

		fig, ax = plt.subplots(figsize=(9.20, 5.28))

		ax.barh(groups, normalized_values1, color = '#ee4d55', label = 'Negative')
		ax.barh(groups, normalized_values2, left = normalized_values1, color='#f6ee54', label = 'Neutral')
		ax.barh(groups, normalized_values3, left = np.add(normalized_values1, normalized_values2), color='#4dab6d', label = 'Positive')
		ax.legend()
		plt.tight_layout()


		# Shrink current axis by 20%
		# box = ax.get_position()
		# ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

		# Put a legend to the right of the current axis
		# ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

	return_val = save_to_buffer(fig)
	plt.close()
	return return_val

def boxplot_as_html(type):
	if type == "Reddit":
		data = query_results["sentiments_boxplot_result"]
	elif type == "News":
		data = query_results["subjectivity_boxplot_result"]
	fig = plt.figure(figsize=(9.20, 5.28))
	sns.boxplot(y = "Keyword",
				x = "Sentiments" if type == "Reddit" else "Subjectivity",
				data = data,
				color = "purple")
	plt.tight_layout()
	plt.subplots_adjust(top=0.9)
	if type == "Reddit":
		plt.title("Distribution of sentiments (comments)", size=15)
	elif type == "News":
		plt.title("Distribution of subjectivity (news articles)", size=15)

	return_val = save_to_buffer(fig)
	plt.close()
	return return_val

def wordcloud_as_html(type):
	if type == "Reddit":
		wc_str = ''.join(query_results["reddit_wordcloud_result"]["comments_body"])
	elif type == "News":
		wc_str = ''.join(pd.concat([query_results["news_wordcloud_result"]["title"], [query_results["news_wordcloud_result"]["text"]]]))
	wc_mask = np.array(PIL.Image.open("./singapore_shape5.png"))
	stopwords = set(list(STOPWORDS) + ["dont", "one", "take", "say", "thing", "make", "will", "didnt", "even", "might", "per", "singapore", "said", "people"])
	wc = WordCloud(stopwords=stopwords,
				   mask=wc_mask,
				   background_color="white",
				   contour_color="black",
				   contour_width=1,
				   min_font_size=3,
				   max_words=400).generate(wc_str)
	fig = plt.figure(figsize=(6.05, 5.46))
	plt.imshow(wc)
	plt.axis("off")

	return_val = save_to_buffer(fig)
	plt.close()
	return return_val

def density_plot_as_html(type):
	if type == "Reddit":
		all_comments_polarity_arr = [(x+1)/2*5 for x in query_results["sentiment_density_plot_result"]['sentiments']]
		fig = plt.figure()
		sns.kdeplot(all_comments_polarity_arr, fill=True, color="purple")
		plt.xlabel("Sentiment", size=12)
		plt.title("Distribution of sentiments (comments)", size=15)

	elif type == "News":
		all_news_subjectivity_arr = [x*5 for x in query_results["subjectivity_density_plot_result"]['sentiments']]
		fig = plt.figure()
		sns.kdeplot(all_news_subjectivity_arr, fill=True, color="purple")
		plt.xticks(np.arange(0, 6, step=1))
		plt.xlabel("Subjectivity", size=12)
		plt.title("Distribution of subjectivity (news articles)", size=15)

	return_val = save_to_buffer(fig)
	plt.close()
	return return_val

def draw_num_comments():
	total_num_of_comments = query_results["total_num_of_comments_result"]["Count"][0]

	return dbc.Row([html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.H1(total_num_of_comments),
					html.H4("Reddit"),
					html.H4("comments")
                ], style={'textAlign': 'center'})
            ]), color="#FF5700"
        ),
    ])], style={"marginBottom": "64px"})

def draw_num_articles():
	total_num_of_articles = query_results["total_num_of_articles_result"]["Count"][0]

	return dbc.Row([html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.H1(total_num_of_articles),
					html.H4("Straits Times"),
					html.H4("articles")
                ], style={'textAlign': 'center', 'color': 'white'})
            ]), color="#283593"
        ),
    ])])

def drawTitle():
	return html.Div([
        dbc.Card(
            dbc.CardBody([
                html.Div([
                    html.H1("Sentiment Analysis (Reddit Comments and Straits Times Articles)"),
                ])
            ]), color="#e2e8f0"
        ),
    ])

# app = Dash(external_stylesheets=[dbc.themes.SLATE])
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    dbc.Card(
        dbc.CardBody([
            dbc.Row([
                drawTitle()
            ], align='center'),
            html.Br(),
            dbc.Row([
                dbc.Col([
					html.H3(["OVERALL COMMENTS SENTIMENT"], style={"backgroundColor": "#f1f5f9"}),
					dbc.Row([
						dbc.Col([
							html.Img(src=gauge_graph_as_html())
						]),
						dbc.Col([
							html.H1([round(query_results["gauge_graph_result"]['Avg_Sentiment'][0], 2)], style={"marginBottom" : "10px", "textAlign": "center", "fontSize": "64px"}),
							html.H2(["out of 5"], style={"marginBottom" : "10px", "textAlign": "center", "fontSize": "40px"}),
							html.H2(["Neutral"], style={"color": "#f6ee54", "textAlign": "center", "fontSize": "56px"}),
						], align='center')
					])
                ], width=5),
                dbc.Col([
					html.H3(["COMMENTS SENTIMENT BREAKDOWN"], style={"backgroundColor": "#f1f5f9"}),
					dbc.Row([
						dbc.Col([
							html.Img(src=donut_graph_as_html())
						]),
						dbc.Col([
							dbc.Row([
								html.H1([str(round(query_results["donut_graph_result"]['Count'][0]/query_results["total_num_of_comments_result"]["Count"][0]*100)) + "%"], style={"color": "#4dab6d", "fontSize": "48px"}),
								html.H2(["positive"], style={"fontSize": "32px"})
							]),
							dbc.Row([
								html.H1([str(round(query_results["donut_graph_result"]['Count'][2]/query_results["total_num_of_comments_result"]["Count"][0]*100)) + "%"], style={"color": "#ee4d55", "fontSize": "48px"}),
								html.H2(["negative"], style={"fontSize": "32px"})
							]),
							dbc.Row([
								html.H1([str(round(query_results["donut_graph_result"]['Count'][1]/query_results["total_num_of_comments_result"]["Count"][0]*100)) + "%"], style={"color": "#f6ee54", "fontSize": "48px"}),
								html.H2(["neutral"], style={"fontSize": "32px"})
							]),
						])
					])
                ], width=5),
                dbc.Col([
                    draw_num_comments(),
					draw_num_articles(),
                ], style={"height": "100%"}, width=2),
            ], align='center'),
            html.Br(),
            dbc.Row([
                dbc.Col([
					html.H3(["COMMENTS SENTIMENT BY KEYWORD"], style={"backgroundColor": "#f1f5f9"}),
					dcc.Tabs(id="tabs-bar-graph", value='tab-1-bar-graph', children=[
						dcc.Tab(label='Relative distribution', value='tab-1-bar-graph'),
						dcc.Tab(label='Absolute distribution', value='tab-2-bar-graph'),
					]),
                    html.Img(id='bar-graph-matplotlib')
                ], width=6),
                dbc.Col([
					html.H3(["BOXPLOT BY KEYWORD"], style={"backgroundColor": "#f1f5f9"}),
					dcc.Tabs(id="tabs-boxplot", value='tab-1-boxplot', children=[
						dcc.Tab(label='Reddit comments', value='tab-1-boxplot'),
						dcc.Tab(label='News articles', value='tab-2-boxplot'),
					]),
                    html.Img(id='boxplot-matplotlib')
                ], width=6),
            ], align='center'),
			html.Br(),
			dbc.Row([
				dbc.Col([
					html.H3(["POSITIVE VS NEGATIVE COMMENTS"], style={"backgroundColor": "#f1f5f9"}),
					html.Img(src=sentiment_by_category_as_html("Scatterplot"))
				], width=4),
				dbc.Col([
					html.H3(["WORD CLOUD"], style={"backgroundColor": "#f1f5f9"}),
					html.Img(src=wordcloud_as_html("Reddit"))
				], width=4),
				dbc.Col([
					html.H3(["DENSITY PLOT"], style={"backgroundColor": "#f1f5f9"}),
					dcc.Tabs(id="tabs-density-plot", value='tab-1-density-plot', children=[
						dcc.Tab(label='Reddit comments', value='tab-1-density-plot'),
						dcc.Tab(label='News articles', value='tab-2-density-plot'),
					]),
					html.Img(id='density-plot-matplotlib')
				], width=4),
			], align='center')
        ]),
    )
])

@app.callback(
	Output(component_id='bar-graph-matplotlib', component_property='src'),
	Output(component_id='boxplot-matplotlib', component_property='src'),
	Output(component_id='density-plot-matplotlib', component_property='src'),
	Input('tabs-bar-graph', 'value'),
	Input('tabs-boxplot', 'value'),
	Input('tabs-density-plot', 'value'),
)
def render_graphs(bar_graph_tab, boxplot_tab, density_plot_tab):
	if bar_graph_tab == 'tab-1-bar-graph':
		bar_graph = sentiment_by_category_as_html("Relative")
	elif bar_graph_tab == 'tab-2-bar-graph':
		bar_graph = sentiment_by_category_as_html("Absolute")

	if boxplot_tab == 'tab-1-boxplot':
		boxplot = boxplot_as_html("Reddit")
	elif boxplot_tab == 'tab-2-boxplot':
		boxplot = boxplot_as_html("News")

	if density_plot_tab == 'tab-1-density-plot':
		density_plot = density_plot_as_html("Reddit")
	elif density_plot_tab == 'tab-2-density-plot':
		density_plot = density_plot_as_html("News")

	return bar_graph, boxplot, density_plot


# @app.callback(
# 	Output(component_id='boxplot-matplotlib', component_property='src'),
# 	Input('tabs-boxplot', 'value'),
# )
# def render_boxplot(tab):
# 	if tab == 'tab-1-boxplot':
# 		return boxplot_as_html("Reddit")
# 	elif tab == 'tab-2-boxplot':
# 		return boxplot_as_html("News")

# @app.callback(
# 	Output(component_id='density-plot-matplotlib', component_property='src'),
# 	Input('tabs-density-plot', 'value'),
# )
# def render_density_plot(tab):
# 	if tab == 'tab-1-density-plot':
# 		return density_plot_as_html("Reddit")
# 	elif tab == 'tab-2-density-plot':
# 		return density_plot_as_html("News")


if __name__ == '__main__':
    app.run_server(debug=False, port=8002)
Copyright © 2011-2024 PythonAnywhere LLP — Terms — Privacy & Cookies
