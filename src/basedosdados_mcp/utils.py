from typing import Optional

# =============================================================================
# Utility Functions
# =============================================================================

def clean_graphql_id(graphql_id: Optional[str]) -> str:
    """
    Clean GraphQL node IDs to extract pure UUIDs.
    
    The API returns IDs like 'DatasetNode:uuid', 'TableNode:uuid', 'ColumnNode:uuid'
    but expects pure UUIDs for queries.
    
    Args:
        graphql_id: GraphQL node ID (e.g., 'DatasetNode:d30222ad-7a5c-4778-a1ec-f0785371d1ca')
        
    Returns:
        Pure UUID string (e.g., 'd30222ad-7a5c-4778-a1ec-f0785371d1ca')
        
    Raises:
        ValueError: If graphql_id is None or empty
    """
    if not graphql_id:
        raise ValueError("GraphQL ID cannot be None or empty")
    
    if ':' in graphql_id:
        return graphql_id.split(':', 1)[1]
    return graphql_id

def normalize_portuguese_accents(text: str) -> str:
    """
    Normalize Portuguese text by adding common accents that users often omit.
    
    This function handles the most common Portuguese accent patterns where users
    type without accents but the database content contains accented characters.
    
    Args:
        text: Input text that may be missing Portuguese accents
        
    Returns:
        Text with common Portuguese accents added where appropriate
        
    Examples:
        "populacao" -> "população"
        "educacao" -> "educação" 
        "saude" -> "saúde"
        "inflacao" -> "inflação"
        "violencia" -> "violência"
    """
    if not text:
        return text
    
    # Common Portuguese accent patterns (most frequent cases)
    accent_patterns = {
        # ação/são pattern endings
        'cao': 'ção',
        'sao': 'são',
        'nao': 'não',
        
        # Common word-specific patterns
        'populacao': 'população',
        'educacao': 'educação', 
        'saude': 'saúde',
        'inflacao': 'inflação',
        'violencia': 'violência',
        'ciencia': 'ciência',
        'experiencia': 'experiência',
        'situacao': 'situação',
        'informacao': 'informação',
        'comunicacao': 'comunicação',
        'administracao': 'administração',
        'organizacao': 'organização',
        'producao': 'produção',
        'construcao': 'construção',
        'operacao': 'operação',
        'participacao': 'participação',
        'avaliacao': 'avaliação',
        'aplicacao': 'aplicação',
        'investigacao': 'investigação',
        'observacao': 'observação',
        'conservacao': 'conservação',
        'preservacao': 'preservação',
        'transformacao': 'transformação',
        'democratica': 'democrática',
        'economica': 'econômica',
        'publica': 'pública',
        'politica': 'política',
        'historica': 'histórica',
        'geografica': 'geográfica',
        'demografica': 'demográfica',
        'academica': 'acadêmica',
        'medica': 'médica',
        'tecnica': 'técnica',
        'biologica': 'biológica',
        'matematica': 'matemática',
        
        # Other common words
        'familia': 'família',
        'historia': 'história',
        'memoria': 'memória',
        'secretaria': 'secretária',
        'area': 'área',
        'energia': 'energia',
        'materia': 'matéria',
        'territorio': 'território',
        'relatorio': 'relatório',
        'laboratorio': 'laboratório',
        'diretorio': 'diretório',
        'repositorio': 'repositório',
        'brasilia': 'brasília',
        'agua': 'água',
        'orgao': 'órgão',
        'orgaos': 'órgãos',
        'opcao': 'opção',
        'opcoes': 'opções',
        'acao': 'ação',
        'acoes': 'ações',
        'regiao': 'região',
        'regioes': 'regiões',
        'estado': 'estado',  # This one typically doesn't need accents
        'municipio': 'município',
        'municipios': 'municípios',
    }
    
    # Create normalized version by applying pattern substitutions
    normalized = text.lower()
    
    # Apply word-level substitutions (exact matches)
    for unaccented, accented in accent_patterns.items():
        if normalized == unaccented:
            return accented
    
    # Apply pattern-based substitutions for partial matches
    for unaccented, accented in accent_patterns.items():
        if unaccented in normalized:
            normalized = normalized.replace(unaccented, accented)
    
    return normalized

def preprocess_search_query(query: str) -> tuple[str, list[str]]:
    """
    Preprocess search queries to handle complex cases and improve search success.
    
    This function cleans and normalizes search queries, handling:
    - Special characters that may cause GraphQL issues
    - Multi-word phrase processing
    - Portuguese accent normalization
    - Query sanitization
    
    Args:
        query: Raw search query from user
        
    Returns:
        tuple of (processed_main_query, fallback_keywords)
        - processed_main_query: Cleaned query for primary search
        - fallback_keywords: Individual keywords for fallback searches
    """
    if not query or not query.strip():
        return "", []
    
    # Clean and normalize the query
    clean_query = query.strip()
    
    # Remove problematic characters for GraphQL
    # Keep Portuguese characters, letters, numbers, spaces, and common punctuation
    import re
    clean_query = re.sub(r'[^\w\sáàâãéêíóôõúçÁÀÂÃÉÊÍÓÔÕÚÇ\-]', ' ', clean_query)
    
    # Normalize multiple spaces to single space
    clean_query = re.sub(r'\s+', ' ', clean_query).strip()
    
    # Apply Portuguese accent normalization
    normalized_query = normalize_portuguese_accents(clean_query)
    
    # Create fallback keywords by splitting the query
    # Remove common stop words that don't add search value
    stop_words = {'de', 'da', 'do', 'das', 'dos', 'e', 'em', 'na', 'no', 'nas', 'nos', 
                  'para', 'por', 'com', 'sem', 'sobre', 'entre', 'the', 'and', 'or', 'of', 'in', 'to'}
    
    # Split into individual words and filter
    words = [word.strip() for word in normalized_query.split() if len(word.strip()) > 2]
    fallback_keywords = [word for word in words if word.lower() not in stop_words]
    
    # Limit fallback keywords to most relevant (first 3-4 words)
    fallback_keywords = fallback_keywords[:4]
    
    return normalized_query, fallback_keywords

def calculate_search_relevance(query: str, dataset: dict) -> float:
    """
    Calculate relevance score for search results to improve ranking.
    
    This implements a scoring system that prioritizes:
    1. Exact matches (especially for acronyms like RAIS, IBGE)
    2. Name matches over description matches
    3. Complete word matches over partial matches
    4. Important/popular datasets
    
    Args:
        query: Original search query
        dataset: Dataset dictionary with name, description, etc.
        
    Returns:
        Float relevance score (higher = more relevant)
    """
    if not query or not dataset:
        return 0.0
    
    score = 0.0
    query_lower = query.lower().strip()
    name = dataset.get('name', '').lower()
    description = dataset.get('description', '').lower()
    slug = dataset.get('slug', '').lower()
    
    # 1. Exact match bonuses (highest priority)
    if query_lower == name.lower():
        score += 100.0  # Perfect name match
    elif query_lower in name.split():
        score += 80.0   # Exact word in name
    elif query_lower == slug:
        score += 200.0  # Perfect slug match - highest priority!
    
    # Special case: if query matches slug exactly, this should be the top result
    if query_lower == slug.replace('_', ' ') or query_lower == slug:
        score += 250.0  # Maximum priority for slug matches
    
    # 2. Acronym and important dataset bonuses
    important_acronyms = {
        'rais': 'relação anual de informações sociais',
        'ibge': 'instituto brasileiro de geografia e estatística',
        'ipea': 'instituto de pesquisa econômica aplicada',
        'inep': 'instituto nacional de estudos e pesquisas educacionais',
        'tse': 'tribunal superior eleitoral',
        'sus': 'sistema único de saúde',
        'pnad': 'pesquisa nacional por amostra de domicílios',
        'pof': 'pesquisa de orçamentos familiares',
        'censo': 'censo demográfico',
        'caged': 'cadastro geral de empregados e desempregados',
        'sinasc': 'sistema de informações sobre nascidos vivos',
        'sim': 'sistema de informações sobre mortalidade'
    }
    
    if query_lower in important_acronyms:
        expected_name = important_acronyms[query_lower]
        # Check if this dataset matches the expected full name
        if any(word in name for word in expected_name.split()):
            score += 150.0  # Major bonus for matching important acronym
        elif any(word in description for word in expected_name.split()):
            score += 100.0  # Good bonus for description match
        
        # Extra bonus if the acronym appears in parentheses in the name (like "RAIS" in the name)
        import re
        acronym_pattern = r'\b' + re.escape(query_lower.upper()) + r'\b'
        if re.search(acronym_pattern, dataset.get('name', '')):
            score += 180.0  # Very high bonus for acronym in name
    
    # 3. Name vs description positioning bonus
    if query_lower in name:
        score += 50.0  # Name contains query
        # Extra bonus for position in name (earlier = better)
        name_words = name.split()
        for i, word in enumerate(name_words):
            if query_lower in word:
                score += max(20.0 - i * 2, 5.0)  # Earlier position = higher bonus
                break
    
    if query_lower in description:
        score += 20.0  # Description contains query
    
    # 4. Word boundary matches (complete words better than substrings)
    import re
    word_pattern = r'\b' + re.escape(query_lower) + r'\b'
    if re.search(word_pattern, name):
        score += 30.0  # Complete word in name
    if re.search(word_pattern, description):
        score += 15.0  # Complete word in description
    
    # 5. Length and specificity bonuses
    if len(query_lower) >= 4:  # Longer queries get bonus for specificity
        score += min(len(query_lower) * 2, 20.0)
    
    # 6. Popular/official source bonuses
    organizations = dataset.get('organizations', '').lower()
    official_sources = ['ibge', 'ipea', 'inep', 'ministério', 'secretaria', 'agência nacional']
    for source in official_sources:
        if source in organizations:
            score += 10.0
            break
    
    # 7. Penalty for very long names (likely less relevant)
    if len(name) > 100:
        score -= 5.0
    
    return score

def rank_search_results(query: str, datasets: list) -> list:
    """
    Rank search results by relevance score.
    
    Args:
        query: Original search query
        datasets: List of dataset dictionaries
        
    Returns:
        List of datasets sorted by relevance (highest first)
    """
    if not datasets:
        return datasets
    
    # Calculate relevance scores
    scored_datasets = []
    for dataset in datasets:
        score = calculate_search_relevance(query, dataset)
        scored_datasets.append((score, dataset))
    
    # Sort by score (descending) and return datasets
    scored_datasets.sort(key=lambda x: x[0], reverse=True)
    return [dataset for score, dataset in scored_datasets]
