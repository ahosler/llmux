import requests
import json
import os
import asyncio
import aiohttp
from typing import List, Dict, Any
import time

class NotebookLMNode:
    """Class representing a NotebookLM instance with its documents"""
    
    def __init__(self, api_key: str, node_id: str):
        self.api_key = api_key
        self.node_id = node_id
        self.base_url = "https://notebooklm.googleapis.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.notebook_id = None
    
    async def create_notebook(self, name: str) -> str:
        """Create a new notebook"""
        url = f"{self.base_url}/notebooks"
        payload = {
            "display_name": f"{name} - Node {self.node_id}"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self.headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    self.notebook_id = data["name"].split("/")[-1]
                    return self.notebook_id
                else:
                    error = await response.text()
                    raise Exception(f"Failed to create notebook: {error}")
    
    async def upload_document(self, document_path: str) -> str:
        """Upload a document to the notebook"""
        if not self.notebook_id:
            raise Exception("Notebook not created yet")
        
        url = f"{self.base_url}/notebooks/{self.notebook_id}/documents"
        
        # Read document content
        with open(document_path, 'rb') as file:
            document_content = file.read()
        
        # Get document name from path
        document_name = os.path.basename(document_path)
        
        # Create multipart form data
        form_data = aiohttp.FormData()
        form_data.add_field('document', document_content, 
                           filename=document_name,
                           content_type='application/octet-stream')
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self.headers, data=form_data) as response:
                if response.status == 200:
                    data = await response.json()
                    return data["document_id"]
                else:
                    error = await response.text()
                    raise Exception(f"Failed to upload document: {error}")
    
    async def ask_question(self, question: str) -> str:
        """Ask a question to the notebook and get the response"""
        if not self.notebook_id:
            raise Exception("Notebook not created yet")
        
        url = f"{self.base_url}/notebooks/{self.notebook_id}/query"
        payload = {
            "query": question
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=self.headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    return data["response"]
                else:
                    error = await response.text()
                    raise Exception(f"Failed to query notebook: {error}")


class NotebookLMScheduler:
    """Scheduler class to coordinate multiple NotebookLM nodes"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.nodes: List[NotebookLMNode] = []
        self.scheduler_node = NotebookLMNode(api_key, "scheduler")
    
    async def initialize_scheduler(self):
        """Initialize the scheduler node"""
        await self.scheduler_node.create_notebook("Scheduler Node")
    
    def add_node(self, node: NotebookLMNode):
        """Add a worker node to the scheduler"""
        self.nodes.append(node)
    
    async def initialize_nodes(self, node_names: List[str]) -> List[NotebookLMNode]:
        """Initialize all worker nodes"""
        nodes = []
        for i, name in enumerate(node_names):
            node = NotebookLMNode(self.api_key, str(i+1))
            await node.create_notebook(name)
            nodes.append(node)
        
        self.nodes = nodes
        return nodes
    
    async def distribute_documents(self, document_paths: List[str], documents_per_node: int = 25):
        """Distribute documents across nodes"""
        if len(document_paths) > len(self.nodes) * documents_per_node:
            raise Exception(f"Too many documents. Maximum is {len(self.nodes) * documents_per_node}")
        
        tasks = []
        for i, node in enumerate(self.nodes):
            start_idx = i * documents_per_node
            end_idx = min(start_idx + documents_per_node, len(document_paths))
            node_docs = document_paths[start_idx:end_idx]
            
            for doc_path in node_docs:
                tasks.append(node.upload_document(doc_path))
        
        # Upload all documents in parallel
        await asyncio.gather(*tasks)
        print(f"Distributed {len(document_paths)} documents across {len(self.nodes)} nodes")
    
    async def process_query(self, question: str) -> str:
        """Process a query by distributing it to all nodes and aggregating responses"""
        print(f"Processing query: {question}")
        
        # Ask each node the question in parallel
        tasks = [node.ask_question(question) for node in self.nodes]
        responses = await asyncio.gather(*tasks)
        
        # Prepare a summary request for the scheduler node
        summary_request = f"""
        I received the following responses to the question: "{question}"
        
        {'-' * 40}
        
        """ + "\n\n".join([f"Node {i+1} Response:\n{response}" for i, response in enumerate(responses)]) + f"""
        
        {'-' * 40}
        
        Please synthesize these responses into a comprehensive answer, highlighting agreements and differences.
        Provide a unified response that captures the key insights from all nodes.
        """
        
        # Ask the scheduler node to synthesize the responses
        final_response = await self.scheduler_node.ask_question(summary_request)
        return final_response


async def main():
    # Replace with your actual API key
    api_key = "YOUR_NOTEBOOKLM_API_KEY"
    
    # Create scheduler
    scheduler = NotebookLMScheduler(api_key)
    await scheduler.initialize_scheduler()
    
    # Initialize worker nodes
    node_names = ["Research Documents", "Financial Documents", "Technical Documents", "Legal Documents"]
    await scheduler.initialize_nodes(node_names)
    
    # Distribute documents (replace with your actual document paths)
    document_paths = [f"document_{i}.pdf" for i in range(1, 101)]
    await scheduler.distribute_documents(document_paths)
    
    # Interactive query loop
    while True:
        question = input("\nEnter your question (or 'exit' to quit): ")
        if question.lower() == 'exit':
            break
        
        start_time = time.time()
        response = await scheduler.process_query(question)
        elapsed_time = time.time() - start_time
        
        print(f"\n{'=' * 80}\nResponse (took {elapsed_time:.2f} seconds):\n{'=' * 80}\n")
        print(response)
        print(f"\n{'=' * 80}")


if __name__ == "__main__":
    asyncio.run(main())
