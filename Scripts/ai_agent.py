
import pandas as pd
from dotenv import load_dotenv        
import os
import json

from langgraph.graph import StateGraph, END
from pydantic import BaseModel

import google.generativeai as genai

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

gemini_api_key = os.getenv("GEMINI_API_KEY")

if not gemini_api_key:
    raise ValueError("GEMINI_API_KEY is missing. Set it in as an environment variable ")

# Configure Gemini client
genai.configure(api_key=gemini_api_key)

# Use Gemini model
llm = genai.GenerativeModel("gemini-2.0-flash")

# -------------------------------
# State Definition
# -------------------------------
class CleaningState(BaseModel):
    input_text: str 
    structured_response: str = ""

# -------------------------------
# AI Agent
# -------------------------------
class AIAgent:
    def __init__(self):
        self.graph = self.create_graph()

    def create_graph(self):
        graph = StateGraph(CleaningState)

        def agent_logic(state: CleaningState) -> CleaningState:
            try:
                response = llm.generate_content(state.input_text)
                response_text = response.text  # Gemini responses are in .text
                return CleaningState(
                    input_text=state.input_text,
                    structured_response=response_text
                )
            except Exception as e:
                return CleaningState(
                    input_text=state.input_text,
                    structured_response=f"ERROR: {str(e)}"
                )
        
        graph.add_node("cleaning_agent", agent_logic)
        graph.add_edge("cleaning_agent", END)
        graph.set_entry_point("cleaning_agent")
        return graph.compile()
    
    def process_data(self, df: pd.DataFrame, batch_size: int = 20):
        cleaned_response = []

        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i: i + batch_size]

            prompt = f"""
            You are an AI Data Cleaning Agent. Analyze the dataset:

            {df_batch.to_string()}
            
            - Identify missing values and impute (mean, mode, median).
            - Remove duplicates.
            - Normalize numeric values.
            - Format text consistently.
            
            Return the cleaned data **only** as valid JSON: a list of dictionaries (rows).
            Example:
            [
                {{"column1": "value1", "column2": 123, "column3": "2023-10-01"}},
                {{"column1": "value2", "column2": 456, "column3": "2023-10-02"}}
            ]
            """

            state = CleaningState(input_text=prompt, structured_response="")
            response = self.graph.invoke(state)

            if isinstance(response, dict):
                response = CleaningState(**response)

            cleaned_response.append(response.structured_response)

        return "\n".join(cleaned_response)
