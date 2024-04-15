import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import flask
import redis
import json
from collections import OrderedDict

# Initialize Flask server
server = flask.Flask(__name__)

# Initialize Redis
r = redis.Redis(host='localhost', port=6379)

# Create a Dash app
app = dash.Dash(__name__, server=server, url_base_pathname='/')

# Define the layout of the app
app.layout = html.Div([
    html.H1("Real-time Stock Prices", style={'textAlign': 'center'}),
    dcc.Graph(id='live-update-graph'),
    html.H1("Historical Stock Prices", style={'textAlign': 'center'}),
    # ... add components for historical data here ...
    dcc.Interval(
        id='interval-component',
        interval=2000,  # in milliseconds
        n_intervals=0
    )
])

# An ordered dictionary to maintain the order of stocks
stock_prices = OrderedDict()

## Define callback to update graph
@app.callback(Output('live-update-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    # Get the latest stock prices from Redis
    data = r.lrange('stock_prices', 0, -1)
    for message in data:
        msg = json.loads(message)
        stock_prices[msg['symbol']] = msg['price']
    
    # Sort the stock symbols to maintain consistent order
    sorted_symbols = sorted(stock_prices.keys())
    sorted_prices = [stock_prices[symbol] for symbol in sorted_symbols]

    # Create the graph with the new data
    trace = go.Scatter(
        x=sorted_symbols,
        y=sorted_prices,
        name='Price',
        mode='lines+markers'
    )

    layout = go.Layout(
        title='Real-Time Stock Prices',
        xaxis=dict(title='Stock Symbol'),
        yaxis=dict(title='Price (USD)'),
        showlegend=False
    )

    return {'data': [trace], 'layout': layout}

if __name__ == '__main__':
    app.run_server(debug=True)