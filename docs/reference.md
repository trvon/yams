---
layout: post
---
Over the past month, I've been working on a project for the [Google ADK agent](https://googlecloudmultiagents.devpost.com/) hackathon. This post provides an overview of my current multi-agent system, used for threat intelligence gathering, processing, and analysis.

The motivation for [Umbrix](https://umbrix.dev) emerged when I was using a small language model to find A LOT of pcaps. I was attempting to seed the model with instructions on how to google dork, and feeded it with search terms to expand find publically reachable network security datasets. From that experiement, it dawned on me, there are many interesting applications for LLM's. From there, timing and motivation was on my side. I set out to build this system with a simple thesis : ***if the future is truly agentic, there are small building blocks and systems that need to be built to improve the efficiently gather and organize sources into a graph.** From here I set out to design / vibe-code a system, able to improve how we fundemntally access information from security feeds by creating agents to efficiently gathering and organized sources into a graph.

## Architecture Overview

The core technical stack includes Kafka, DSPy, Neo4j for relationship mapping and entity resolution, Prometheus/Grafana for metrics and monitoring, and many specialized MCP tools for AI agent integration.

![Umbrix Control Loop Architecture](https://manta.black/assets/umbrix-control-loop.svg)

## The Agent Control Loop

The control loop architecture coordinates multiple specialized agents through an event-driven system with intelligent routing and failure recovery. Here's a small snippet of how the master coordinator manages the agent lifecycle:

```python
class CTIMasterCoordinatorAgent(Agent):
    """Enhanced Master coordinator with task validation and structured logging"""

    def __init__(self, name="cti_master_coordinator", kafka_topic="agent.tasks", **kwargs):
        super().__init__(name, **kwargs)
        self.kafka_topic = kafka_topic
        self.agent_registry = {}
        self.task_queue = asyncio.Queue()

        # Initialize specialized agents with health monitoring
        self.discovery_agents = [
            RssDiscoveryAgent(coordination_topic="coordination.tasks"),
            FeedSeederAgent(coordination_topic="coordination.tasks")
        ]

        self.collection_agents = [
            EnhancedRssCollectorAgent(coordination_topic="coordination.tasks"),
            TaxiiPullAgent(coordination_topic="coordination.tasks") if TaxiiPullAgent else None,
            ShodanStreamAgent(coordination_topic="coordination.tasks") if ShodanStreamAgent else None
        ]

        self.processing_agents = [
            FeedNormalizationAgent(coordination_topic="coordination.tasks"),
            GeoIpEnrichmentAgent(coordination_topic="coordination.tasks"),
            AdvancedGraphLibrarianAgent(coordination_topic="coordination.tasks")
        ]

    async def coordinate_agent_lifecycle(self):
        """Main coordination loop with health monitoring and failure recovery"""

        while True:
            try:
                # Health check all agents
                for agent_group in [self.discovery_agents, self.collection_agents, self.processing_agents]:
                    for agent in agent_group:
                        if agent and not await self.check_agent_health(agent):
                            await self.restart_agent(agent)

                # Process coordination tasks
                if not self.task_queue.empty():
                    task = await self.task_queue.get()
                    await self.route_task_to_agent(task)

                # Monitor Kafka lag and adjust agent scaling
                await self.monitor_processing_lag()

                await asyncio.sleep(5)  # Coordination cycle interval

            except Exception as e:
                self.logger.error(f"Coordination error: {e}")
                await asyncio.sleep(10)  # Recovery delay

    async def route_task_to_agent(self, task):
        """Intelligent task routing based on content type and agent availability"""

        task_type = task.get('type')
        content_confidence = task.get('content_analysis', {}).get('confidence', 0.5)

        # Route based on content analysis and agent specialization
        if task_type == 'feed_discovery':
            agent = self.select_best_available_agent(self.discovery_agents)
        elif task_type == 'content_collection':
            # Use enhanced collector for high-confidence security content
            if content_confidence > 0.7:
                agent = self.find_agent_by_type(EnhancedRssCollectorAgent)
            else:
                agent = self.select_best_available_agent(self.collection_agents)
        elif task_type == 'content_processing':
            agent = self.select_best_available_agent(self.processing_agents)

        if agent:
            await agent.process_task(task)
        else:
            # Queue task for retry if no agents available
            await self.task_queue.put(task)
```

Discovery agents implement a sophisticated feedback loop for identifying high-quality threat intelligence sources. The system uses [DSPy](https://dspy.ai) to analyze feed content patterns, publication frequency, IOC density, and attribution quality. Sources scoring above 0.7 confidence are automatically added to collection queues. The quality scoring algorithm evaluates multiple dimensions: content relevance to cybersecurity topics, presence of structured threat indicators, attribution to known threat actors, publication consistency, and source reputation (this needs to be improved). This automated process ensures the system focuses collection efforts on high-value data. Our RSS collector, for examples, uses structure-preserving content extraction that maintains document hierarchy—headers, sections, lists—enabling DSPy to understand contextual relationships.

Here's how the process works in practice:

```python
# Enhanced feed processing with intelligent routing
def _process_feed_entry(self, entry, feed_url):
    # Create structured feed record with metadata preservation
    feed_record = FeedRecord(
        url=AnyUrl(entry_link_str),
        title=entry.get('title'),
        description=entry.get('summary'),
        published_at=self._parse_feed_date(entry.get('published')),
        discovered_at=datetime.now(timezone.utc),
        source_name=urlparse(feed_url).netloc,
        source_type='rss',
        tags=[tag.get('term') for tag in entry.get('tags', []) if tag.get('term')],
        raw_content=None,
        raw_content_type='html'
    )

    # Extract content with retry framework
    extraction_result = self._extract_content_with_retry(entry_link_str)

    # Store structured extraction results
    feed_record.raw_content = extraction_result.get('raw_html')
    setattr(feed_record.metadata, 'extracted_clean_text', extraction_result.get('text'))
    setattr(feed_record.metadata, 'extraction_quality', extraction_result.get('extraction_quality'))
    setattr(feed_record.metadata, 'extraction_method', extraction_result.get('extraction_method'))
    setattr(feed_record.metadata, 'extraction_confidence', extraction_result.get('extraction_confidence'))

    # Intelligent content analysis for routing
    if self._should_use_enhanced_enrichment(normalized_record):
        content_type = normalized_record.metadata.content_analysis['content_type']
        if content_type in ['security_threat', 'security_advisory', 'potential_security']:
            enrichment_method = "advanced_security"
        elif content_type in ['api_documentation', 'security_tools']:
            enrichment_method = "technical"
        elif content_type in ['mixed_content', 'unclear']:
            enrichment_method = "hybrid"
        else:
            enrichment_method = "basic"
```

Our pipeline preserves the document's organizational structure, which improves analysis accuracy. For example, when processing a security advisory, the system maintains separate sections for technical analysis, attribution, and indicators, allowing DSPy modules to apply specialized analysis to each component. The system extracts structured threat intelligence including specific file hashes (fc8b2c9e3d4a1b5c), executable names (badlock.exe), and malware family classifications. This structured approach allows DSPy modules to apply appropriate analysis techniques to each content type. Headlines indicate content type, technical sections contain indicators, and attribution sections provide threat actor context. The core utility comes from our intelligent entity extraction system that uses pattern matching combined with context analysis:

```python
def _extract_entities(self, text: str) -> Dict[str, List[str]]:
    """Extract cybersecurity entities using pattern matching"""

    entities = {
        'hashes': [],
        'ips': [],
        'domains': [],
        'vulnerabilities': [],
        'threat_actors': []
    }

    # Extract file hashes (MD5, SHA1, SHA256)
    hash_patterns = [
        r'\b[a-fA-F0-9]{32}\b',    # MD5
        r'\b[a-fA-F0-9]{40}\b',    # SHA1
        r'\b[a-fA-F0-9]{64}\b'     # SHA256
    ]
    for pattern in hash_patterns:
        entities['hashes'].extend(re.findall(pattern, text))

    # Extract IP addresses with context validation
    ip_pattern = r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'
    potential_ips = re.findall(ip_pattern, text)
    # Filter out common false positives like version numbers
    entities['ips'] = [ip for ip in potential_ips if not ip.startswith('1.0')]

    # Extract domain names with TLD validation
    domain_pattern = r'\b[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\.[a-zA-Z]{2,}\b'
    entities['domains'] = re.findall(domain_pattern, text)

    # Extract CVE identifiers
    cve_pattern = r'CVE-\d{4}-\d{4,7}'
    entities['vulnerabilities'] = list(set(re.findall(cve_pattern, text, re.IGNORECASE)))

    # Extract threat actor names using known patterns
    actor_pattern = r'\b(?:APT\d+|FIN\d+|[A-Z][a-z]+\s*(?:Bear|Panda|Tiger|Spider|Kitten))\b'
    entities['threat_actors'] = list(set(re.findall(actor_pattern, text)))

    return entities

def _calculate_security_confidence(self, security_score, context_matches, entities, phrase_matches, non_security_score):
    """Calculate overall confidence using weighted scoring"""

    # Base score from keyword analysis (30% weight)
    confidence = security_score * 0.3

    # Context patterns boost (25% weight)
    context_boost = min(0.25, context_matches * 0.05)
    confidence += context_boost

    # Entity detection boost (30% weight)
    entity_boost = 0.0
    if entities['vulnerabilities']:
        entity_boost += 0.15  # CVEs are strong indicators
    if entities['threat_actors']:
        entity_boost += 0.15  # APT groups are strong indicators
    if entities['hashes'] or entities['ips']:
        entity_boost += 0.10  # IOCs are moderate indicators
    confidence += min(0.3, entity_boost)

    # Security phrase boost (15% weight)
    phrase_boost = min(0.15, phrase_matches * 0.03)
    confidence += phrase_boost

    # Non-security content penalty
    confidence -= non_security_score * 0.5

    return max(0.0, min(1.0, confidence))
```

The processing pipeline implements several specialized enrichment stages. GeoIP enrichment agents analyze IP indicators to determine country of origin, ASN information, and threat reputation scores. MITRE ATT&CK mapping agents identify tactics and techniques based on behavior patterns described in threat reports. Each processing stage adds contextual information that enables more sophisticated analysis downstream. Geographic attribution helps identify threat actor regions, while MITRE mappings provide standardized threat categorization that facilitates cross-reference analysis.

The entire system operates through event-driven architecture using Kafka topics to decouple agents and enable horizontal scaling. The message flow follows: feeds.discovered → raw.intel → enriched.intel → graph.events, with each stage handled by specialized agent types. Topic specifications include feeds.discovered for new feed URLs with quality scores, raw.intel for unprocessed threat intelligence content, enriched.intel for DSPy-enhanced intelligence with IOCs and attribution, graph.events for Neo4j operations, agent.errors as a dead letter queue for failed processing, and coordination.tasks for master coordinator agent orchestration.

The system implements exactly-once processing through manual commit control and correlation-based deduplication. Each message includes a correlation ID that tracks processing across the entire pipeline. Duplicate messages are automatically filtered, and commits only occur after successful processing completion. This approach ensures data integrity across the distributed system. Message processing failures don't result in data loss, and successful processing guarantees exactly-once delivery to downstream systems.


The Neo4j graph database functions as an active intelligence layer that maintains relationships. The system resolves entities to canonical forms, creates relationship patterns based on context and temporal data, and executes graph mutations that build comprehensive threat intelligence networks. Entity resolution handles variations in indicator representation—different formats for the same IP address, domain, or file hash. We also created a "Graph Librarian" agent to infer connections based on co-occurrence in reports, shared infrastructure, similar TTPs, and temporal correlation patterns. The same agent analyzes query patterns and suggests indexes for improved performance. We have some secret sauce that we are still iterating on in this agent, so well have to share some more details shared later.

We provide a MCP client with specialized MCP tools to allow agents to access our graph. The MCP interface supports complex queries like "ransomware targeting healthcare in the last 30 days," "APT groups using spear phishing against financial institutions," and "infrastructure overlap between Lazarus and APT38." The system converts natural language queries to optimized Cypher graph queries and returns structured results with execution metadata. Hopefully, as the amount of nodes in the graph scales, we will see cool use cases or retrieving useful threat intelligence. For now, simplying quering for the latest ios vunlerbility retrieves our most update to date graph date.

For those interested in the technical implementation details, the platform includes comprehensive documentation for MCP integration, API usage, and deployment configurations. The MCP tools are available through our [documentation portal](https://umbrix.dev/docs.html).


---
layout: post
title: Umbrix Platform Update: Entity Extraction Evolution and Cost Optimization
---


So I wanted to post a follow-up to my previous introduction to the Umbrix platform, where I was using DSPy for entity extraction in cyber threat intelligence. If you missed that, you can find it [here](https://manta.black/leveraging-google-adk-for-cyber-intelligence.html). This recap comes after a week after I launched the platform.


After spending about 2 days getting Umbrix running optimally, I started running many problems. I have been extracting a significant volume of entities, relationships, and nodes from my agents to populate in the graph. However, I quickly realized my system was burning through resources - both in terms of rate limits and agent costs.


The original pipeline was elegant but expensive - every piece of content went through DSPy-powered extraction using the `_extract_entities` method I showed in my previous post. While this gave us high-quality extraction with contextual understanding, processing thousands of feeds daily meant burning through tokens at an unsustainable rate.


At that point, I pivoted to optimize for the highest throughput at the lowest cost. The question became: what's the most efficient LLM configuration for this specific entity extraction task?


Since I was hosting on GCP, I had access to various model options. While the performance differences for entity extraction were minimal between models, the cost differences were substantial. But honestly, it didn't matter much because using LLMs for high-volume entity extraction proved expensive regardless of configuration.


This realization led me to step back and review the literature on state-of-the-art entity extraction techniques. My research pointed toward exploring spaCy and BERT models for entity extraction, which could potentially maintain quality while drastically reducing costs.


## The Evolution: From Pure LLM to Hybrid Pipeline


We started with experiments in gradient balancing and multi-task learning, testing different approaches to optimize our extraction pipeline.


The new pipeline architecture leverages a tiered approach:


### Tier 1: High-Volume Pattern Matching
First, we run lightweight regex-based extraction for well-defined patterns. This catches the obvious stuff - IPs, domains, hashes, CVEs - without any model inference:


```python
# From our optimized pipeline
def tier1_extraction(text):
    """Ultra-fast pattern matching for common indicators"""
    entities = {
        'hashes': extract_hashes_regex(text),
        'ips': extract_ips_regex(text),
        'domains': extract_domains_regex(text),
        'cves': extract_cves_regex(text)
    }
    return entities
```


### Tier 2: spaCy NER for Named Entities
For threat actors, malware families, and organizations, we use a custom-trained spaCy model. We fine-tuned it on cybersecurity text using data from our initial DSPy extractions.


```python
# Custom spaCy pipeline for cyber entities
nlp = spacy.load("en_cybersec_ner_v2")
nlp.add_pipe("entity_ruler", before="ner")


# Add patterns for common threat actor naming conventions
patterns = [
    {"label": "THREAT_ACTOR", "pattern": [{"TEXT": {"REGEX": "APT\d+"}}]},
    {"label": "THREAT_ACTOR", "pattern": [{"LOWER": {"IN": ["lazarus", "apt28", "cozy bear"]}}]},
    {"label": "MALWARE", "pattern": [{"LOWER": {"IN": ["emotet", "trickbot", "ryuk"]}}]}
]
ruler = nlp.get_pipe("entity_ruler")
ruler.add_patterns(patterns)
```


### Tier 3: BERT-based Classification and Disambiguation
Here's where it gets interesting. We use a fine-tuned BERT model for two critical tasks:
1. **Entity Type Classification**: When spaCy isn't confident, BERT classifies ambiguous entities
2. **Relationship Extraction**: BERT identifies relationships between entities in context


The BERT implementation leverages our multi-task training framework from the experiments:


```python
# Simplified version of our BERT integration
class CyberEntityBERT:
    def __init__(self):
        self.model = AutoModel.from_pretrained("umbrix/cyber-entity-bert")
        self.tokenizer = AutoTokenizer.from_pretrained("umbrix/cyber-entity-bert")
       
    def classify_entity_context(self, text, entity):
        """Classify entity type based on surrounding context"""
        # Encode text with entity markers
        marked_text = text.replace(entity, f"[ENTITY]{entity}[/ENTITY]")
        inputs = self.tokenizer(marked_text, return_tensors="pt", truncation=True)
       
        with torch.no_grad():
            outputs = self.model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
           
        return self._decode_predictions(predictions)
```


### Tier 4: LLM for Complex Reasoning (Selective)
Only the most ambiguous or complex cases make it to LLM processing now. We're talking about:
- Novel threat actor attribution requiring reasoning
- Complex attack chain analysis
- Zero-day vulnerability impact assessment


This selective approach reduced our LLM costs by approximately 94% while maintaining extraction quality above 92% compared to pure DSPy. Unfortunately, as a student these costs are unsustainable, so I disabled this feature extraction.


## The Graph Management Challenge


Another issue that emerged was that my Graph Librarian agent was being too aggressive in creating entity connections. The graph was becoming overly connected - too many relationships were being inferred, which reduced the signal-to-noise ratio.


The solution came from implementing a confidence-weighted relationship scoring system:


```python
class EnhancedGraphLibrarian:
    def __init__(self):
        self.relationship_threshold = 0.7
        self.co_occurrence_window = 150  # tokens
       
    def score_relationship(self, entity1, entity2, context):
        """Score potential relationship based on multiple factors"""
        scores = {
            'proximity': self._calculate_proximity_score(entity1, entity2, context),
            'semantic': self._calculate_semantic_similarity(entity1, entity2),
            'temporal': self._calculate_temporal_correlation(entity1, entity2),
            'type_compatibility': self._check_relationship_validity(entity1.type, entity2.type)
        }
       
        # Weighted average with type compatibility as a gate
        if scores['type_compatibility'] < 0.3:
            return 0.0
           
        weighted_score = (
            scores['proximity'] * 0.3 +
            scores['semantic'] * 0.4 +
            scores['temporal'] * 0.3
        )
       
        return weighted_score
```


From this perspective, I figured it was a good time to use LLMs to analyze my system architecture and research state-of-the-art approaches from adjacent fields - particularly medical research entity extraction, biomedical text analysis, and ML pipelines. The goal was to build something that's both:
- More efficient (lower cost, higher throughput)
- More accurate (better precision in entity extraction and relationship inference)
- Better at graph management (quality over quantity in connections)


## Current State and Next Steps


This brings us to where we are now. We're implementing a hybrid approach that combines the aforementioned techniques and a new correlation analysis module that tracks entity relationships across time. For now, we will keep working and plan to release more details on our platform soon.

