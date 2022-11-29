import dash
import dash_bootstrap_components as dbc

sq_support = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], title='Square Pool Cleaning')

sq_support.config.suppress_callback_exceptions = True

server = sq_support.server
server.config['SECRET_KEY'] = 'twyt1cubt!eswip7892'