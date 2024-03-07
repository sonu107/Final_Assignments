#Installing and upgrading the necessary modules.
pip install --upgrade --quiet  langchain langchain-community langchain-experimental

#Import required libraries.
from langchain_community.utilities.sql_database import SQLDatabase

#Define the variables for connecting to a PostgreSQL database.
database = "Your_database_name"  
username = "User_name"
password = "Passowrd"
host = "localhost"
port = '5433'

#Define the connection string for connceting to a PostgreSQL Database.
db_uri = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

#Connection to the PostgreSQL database specified by the URI.
db = SQLDatabase.from_uri(db_uri)

#Import necessary libraries.
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import ChatOpenAI
import os

#Set up your OpenAI API key.
os.environ["OPENAI_API_KEY"] = "your_api_key"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

#Define llm using ChatOpenAI.
llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)

#Create an SQL Agent.
agent_executor = create_sql_agent(llm, db=db, agent_type="openai-tools", verbose=True)

#Invoke the agent which will interact with the SQL agent and execute SQL queries against the connected database.
agent_executor.invoke(
    "Calculate average departure delay as per months from flight table"
)

agent_executor.invoke({"Calculate average departure and arrival delay for all flights from the flight table."})

#TOOLKIT
#We can look at what runs under the hood with the create_sql_agent helper above.
#We can also highlight how the toolkit is used with the agent specifically.

from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI

toolkit = SQLDatabaseToolkit(db=db, llm=ChatOpenAI(temperature=0))
context = toolkit.get_context()
tools = toolkit.get_tools()

print(context)
print(tools)

#Use SQLDatabaseToolkit within an Agent.
from langchain_community.agent_toolkits.sql.prompt import SQL_FUNCTIONS_SUFFIX
from langchain_core.messages import AIMessage, SystemMessage
from langchain_core.prompts.chat import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    MessagesPlaceholder,
)

messages = [
    HumanMessagePromptTemplate.from_template("{input}"),
    AIMessage(content=SQL_FUNCTIONS_SUFFIX),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
]

prompt = ChatPromptTemplate.from_messages(messages)
prompt = prompt.partial(**context)

print(prompt)

from langchain.agents import create_openai_tools_agent
from langchain.agents.agent import AgentExecutor

llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)

agent = create_openai_tools_agent(llm, tools, prompt)

agent_executor = AgentExecutor(
    agent=agent,
    tools=toolkit.get_tools(),
    verbose=True,
)

agent_executor.invoke({"input": "Describe the schema of the flight table"})