import os
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential
from azure.ai.projects.models import FunctionTool, ToolSet
from typing import Any, Callable, Set

# Import your custom functions
from tools.get_learning_path_tool import assesment_functions
from dotenv import load_dotenv

load_dotenv()  # Ensure your connection string is stored here

def assesment_agent(agent_id: str, prompt: str):
    """
    Creates an Azure AI Agent with a custom function toolset and processes a user prompt.
    
    Args:
        agent_id (str): The ID of the agent to use.
        prompt (str): The user prompt to send to the agent.
    """
    # Initialize the Azure AI Project Client
    connection_string = os.getenv("AZURE_AI_PROJECT_CONNECTION_STRING")
    project_client = AIProjectClient.from_connection_string(
        credential=DefaultAzureCredential(),
        conn_str=connection_string,
    )

    # Initialize the toolset with custom functions
    functions = FunctionTool(assesment_functions)  # Add your custom functions
    toolset = ToolSet()
    toolset.add(functions)

    # Retrieve the agent
    agent = project_client.agents.get_agent(agent_id)
    print(f"Retrieved agent, ID: {agent.id}")

    # Create a thread for communication
    thread = project_client.agents.create_thread()
    print(f"Created thread, ID: {thread.id}")

    # Create a message in the thread
    message = project_client.agents.create_message(
        thread_id=thread.id,
        role="user",
        content=prompt,
    )
    print(f"Created message, ID: {message.id}")

    # Create and process a run with the agent and toolset
    run = project_client.agents.create_and_process_run(
        thread_id=thread.id,
        agent_id=agent.id,
        toolset=toolset,
    )
    print(f"Run finished with status: {run.status}")

    # Check for errors
    if run.status == "failed":
        print(f"Run failed: {run.last_error}")
        return

    # Fetch and log all messages in the thread
    messages = project_client.agents.list_messages(thread_id=thread.id)
    for msg in messages:
        print(f"Message from {msg.role}: {msg.content}")

    # Retrieve the latest response from the agent
    last_message = project_client.agents.get_last_message_text_by_role(
        thread_id=thread.id,
        role="agent",
    )
    if last_message:
        print(f"Agent response: {last_message.text.value}")

# Example usage
if __name__ == "__main__":
    # Replace with your agent ID and user prompt
    agent_id = "your_agent_id"
    user_prompt = ""
    assesment_agent(agent_id, user_prompt)
