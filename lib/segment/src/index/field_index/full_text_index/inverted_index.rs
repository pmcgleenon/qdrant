use std::collections::{BTreeSet, HashMap};

use common::types::PointOffsetType;
use serde::{Deserialize, Serialize};

use super::posting_list::PostingList;
use super::postings_iterator::intersect_postings_iterator;
use crate::common::operation_error::{OperationError, OperationResult};
use crate::index::field_index::{CardinalityEstimation, PayloadBlockCondition, PrimaryCondition};
use crate::types::{FieldCondition, Match, MatchText, PayloadKeyType};

pub type TokenId = u32;

#[derive(Default, Serialize, Deserialize, Debug, Clone)]
pub struct Document {
    tokens: Vec<TokenId>,
}

impl Document {
    pub fn new(mut tokens: Vec<TokenId>) -> Self {
        tokens.sort_unstable();
        Self { tokens }
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    pub fn tokens(&self) -> &[TokenId] {
        &self.tokens
    }

    pub fn check(&self, token: TokenId) -> bool {
        self.tokens.binary_search(&token).is_ok()
    }
}

#[derive(Debug)]
pub struct ParsedQuery {
    pub tokens: Vec<Option<TokenId>>,
}

impl ParsedQuery {
    pub fn check_match(&self, document: &Document) -> bool {
        if self.tokens.contains(&None) {
            return false;
        }
        // Check that all tokens are in document
        self.tokens
            .iter()
            // unwrap crash safety: all tokens exist in the vocabulary if it passes the above check
            .all(|query_token| document.check(query_token.unwrap()))
    }
}

pub enum InvertedIndex {
    Mutable(MutableInvertedIndex),
    Immutable(ImmutableInvertedIndex),
}

impl InvertedIndex {
    pub fn new(is_appendable: bool) -> InvertedIndex {
        if is_appendable {
            InvertedIndex::Mutable(MutableInvertedIndex::default())
        } else {
            InvertedIndex::Immutable(ImmutableInvertedIndex::default())
        }
    }

    pub fn document_from_tokens(&mut self, tokens: &BTreeSet<String>) -> Document {
        let vocab = match self {
            InvertedIndex::Mutable(index) => &mut index.vocab,
            InvertedIndex::Immutable(index) => &mut index.vocab,
        };
        let mut document_tokens = vec![];
        for token in tokens {
            // check if in vocab
            let vocab_idx = match vocab.get(token) {
                Some(&idx) => idx,
                None => {
                    let next_token_id = vocab.len() as TokenId;
                    vocab.insert(token.to_string(), next_token_id);
                    next_token_id
                }
            };
            document_tokens.push(vocab_idx);
        }

        Document::new(document_tokens)
    }

    pub fn index_document(
        &mut self,
        idx: PointOffsetType,
        document: Document,
    ) -> OperationResult<()> {
        match self {
            InvertedIndex::Mutable(index) => index.index_document(idx, document),
            InvertedIndex::Immutable(_index) => Err(OperationError::service_error(
                "Can't add values to immutable text index",
            )),
        }
    }

    pub fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        match self {
            InvertedIndex::Mutable(index) => index.remove_document(idx),
            InvertedIndex::Immutable(index) => index.remove_document(idx),
        }
    }

    pub fn filter(&self, query: &ParsedQuery) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        let index_postings = match self {
            InvertedIndex::Mutable(index) => &index.postings,
            InvertedIndex::Immutable(index) => &index.postings,
        };
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| match vocab_idx {
                None => None,
                // if a ParsedQuery token was given an index, then it must exist in the vocabulary
                // dictionary. Posting list entry can be None but it exists.
                Some(idx) => index_postings.get(idx as usize).unwrap().as_ref(),
            })
            .collect();
        if postings_opt.is_none() {
            // There are unseen tokens -> no matches
            return Box::new(vec![].into_iter());
        }
        let postings = postings_opt.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return Box::new(vec![].into_iter());
        }
        intersect_postings_iterator(postings)
    }

    pub fn estimate_cardinality(
        &self,
        query: &ParsedQuery,
        condition: &FieldCondition,
    ) -> CardinalityEstimation {
        let points_count = match self {
            InvertedIndex::Mutable(index) => index.points_count,
            InvertedIndex::Immutable(index) => index.points_count,
        };
        let index_postings = match self {
            InvertedIndex::Mutable(index) => &index.postings,
            InvertedIndex::Immutable(index) => &index.postings,
        };
        let postings_opt: Option<Vec<_>> = query
            .tokens
            .iter()
            .map(|&vocab_idx| match vocab_idx {
                None => None,
                // unwrap safety: same as in filter()
                Some(idx) => index_postings.get(idx as usize).unwrap().as_ref(),
            })
            .collect();
        if postings_opt.is_none() {
            // There are unseen tokens -> no matches
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        let postings = postings_opt.unwrap();
        if postings.is_empty() {
            // Empty request -> no matches
            return CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0,
                exp: 0,
                max: 0,
            };
        }
        // Smallest posting is the largest possible cardinality
        let smallest_posting = postings.iter().map(|posting| posting.len()).min().unwrap();

        return if postings.len() == 1 {
            CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: smallest_posting,
                exp: smallest_posting,
                max: smallest_posting,
            }
        } else {
            let expected_frac: f64 = postings
                .iter()
                .map(|posting| posting.len() as f64 / points_count as f64)
                .product();
            let exp = (expected_frac * points_count as f64) as usize;
            CardinalityEstimation {
                primary_clauses: vec![PrimaryCondition::Condition(condition.clone())],
                min: 0, // ToDo: make better estimation
                exp,
                max: smallest_posting,
            }
        };
    }

    pub fn payload_blocks(
        &self,
        threshold: usize,
        key: PayloadKeyType,
    ) -> Box<dyn Iterator<Item = PayloadBlockCondition> + '_> {
        // It might be very hard to predict possible combinations of conditions,
        // so we only build it for individual tokens
        let vocab = match self {
            InvertedIndex::Mutable(index) => &index.vocab,
            InvertedIndex::Immutable(index) => &index.vocab,
        };
        let postings = match self {
            InvertedIndex::Mutable(index) => &index.postings,
            InvertedIndex::Immutable(index) => &index.postings,
        };
        Box::new(
            vocab
                .iter()
                .filter(|(_token, &posting_idx)| postings[posting_idx as usize].is_some())
                .filter(move |(_token, &posting_idx)| {
                    // unwrap crash safety: all tokens that passes the first filter should have postings
                    postings[posting_idx as usize].as_ref().unwrap().len() >= threshold
                })
                .map(|(token, &posting_idx)| {
                    (
                        token,
                        // same as the above case
                        postings[posting_idx as usize].as_ref().unwrap(),
                    )
                })
                .map(move |(token, posting)| PayloadBlockCondition {
                    condition: FieldCondition {
                        key: key.clone(),
                        r#match: Some(Match::Text(MatchText {
                            text: token.clone(),
                        })),
                        range: None,
                        geo_bounding_box: None,
                        geo_radius: None,
                        geo_polygon: None,
                        values_count: None,
                    },
                    cardinality: posting.len(),
                }),
        )
    }

    pub fn build_index(
        &mut self,
        iter: impl Iterator<Item = OperationResult<(PointOffsetType, BTreeSet<String>)>>,
    ) -> OperationResult<()> {
        let mut index = MutableInvertedIndex::default();
        for i in iter {
            let (idx, tokens) = i?;
            let doc = self.document_from_tokens(&tokens);
            index.index_document(idx, doc)?;
        }

        match self {
            InvertedIndex::Mutable(i) => {
                *i = index;
            }
            InvertedIndex::Immutable(i) => {
                *i = index.into();
            }
        }

        Ok(())
    }

    pub fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        match self {
            InvertedIndex::Mutable(index) => index.check_match(parsed_query, point_id),
            InvertedIndex::Immutable(index) => index.check_match(parsed_query, point_id),
        }
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        match self {
            InvertedIndex::Mutable(index) => index.values_is_empty(point_id),
            InvertedIndex::Immutable(index) => index.values_is_empty(point_id),
        }
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        match self {
            InvertedIndex::Mutable(index) => index.values_count(point_id),
            InvertedIndex::Immutable(index) => index.values_count(point_id),
        }
    }

    pub fn points_count(&self) -> usize {
        match self {
            InvertedIndex::Mutable(index) => index.points_count,
            InvertedIndex::Immutable(index) => index.points_count,
        }
    }

    pub fn get_token(&self, token: &str) -> Option<TokenId> {
        match self {
            InvertedIndex::Mutable(index) => index.vocab.get(token).copied(),
            InvertedIndex::Immutable(index) => index.vocab.get(token).copied(),
        }
    }
}

#[derive(Default)]
pub struct MutableInvertedIndex {
    postings: Vec<Option<PostingList>>,
    vocab: HashMap<String, TokenId>,
    point_to_docs: Vec<Option<Document>>,
    points_count: usize,
}

impl MutableInvertedIndex {
    pub fn index_document(
        &mut self,
        idx: PointOffsetType,
        document: Document,
    ) -> OperationResult<()> {
        self.points_count += 1;
        if self.point_to_docs.len() <= idx as usize {
            self.point_to_docs
                .resize_with(idx as usize + 1, Default::default);
        }

        for token_idx in document.tokens() {
            let token_idx_usize = *token_idx as usize;
            if self.postings.len() <= token_idx_usize {
                self.postings
                    .resize_with(token_idx_usize + 1, Default::default);
            }
            let posting = self
                .postings
                .get_mut(token_idx_usize)
                .expect("posting must exist even if with None");
            match posting {
                None => *posting = Some(PostingList::new(idx)),
                Some(vec) => vec.insert(idx),
            }
        }
        self.point_to_docs[idx as usize] = Some(document);
        Ok(())
    }

    pub fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        if self.point_to_docs.len() <= idx as usize {
            return false; // Already removed or never actually existed
        }

        let removed_doc = match std::mem::take(&mut self.point_to_docs[idx as usize]) {
            Some(doc) => doc,
            None => return false,
        };

        self.points_count -= 1;

        for removed_token in removed_doc.tokens() {
            // unwrap safety: posting list exists and contains the document id
            let posting = self.postings.get_mut(*removed_token as usize).unwrap();
            if let Some(vec) = posting {
                vec.remove(idx);
            }
        }
        true
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        // Maybe we want number of documents in the future?
        self.get_doc(point_id).map(|x| x.len()).unwrap_or(0)
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        self.get_doc(point_id).map(|x| x.is_empty()).unwrap_or(true)
    }

    pub fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        if let Some(doc) = self.get_doc(point_id) {
            parsed_query.check_match(doc)
        } else {
            false
        }
    }

    fn get_doc(&self, idx: PointOffsetType) -> Option<&Document> {
        self.point_to_docs.get(idx as usize)?.as_ref()
    }
}

#[derive(Default)]
pub struct ImmutableInvertedIndex {
    postings: Vec<Option<PostingList>>,
    vocab: HashMap<String, TokenId>,
    point_documents_tokens: Vec<Option<usize>>,
    points_count: usize,
}

impl ImmutableInvertedIndex {
    pub fn remove_document(&mut self, idx: PointOffsetType) -> bool {
        if self.values_is_empty(idx) {
            return false; // Already removed or never actually existed
        }
        self.point_documents_tokens[idx as usize] = None;
        self.points_count -= 1;
        true
    }

    pub fn values_is_empty(&self, point_id: PointOffsetType) -> bool {
        if self.point_documents_tokens.len() <= point_id as usize {
            return true;
        }
        self.point_documents_tokens[point_id as usize].is_none()
    }

    pub fn values_count(&self, point_id: PointOffsetType) -> usize {
        if self.point_documents_tokens.len() <= point_id as usize {
            return 0;
        }
        self.point_documents_tokens[point_id as usize].unwrap_or(0)
    }

    pub fn check_match(&self, parsed_query: &ParsedQuery, point_id: PointOffsetType) -> bool {
        if parsed_query.tokens.contains(&None) {
            return false;
        }
        if self.values_is_empty(point_id) {
            return false;
        }
        // Check that all tokens are in document
        parsed_query
            .tokens
            .iter()
            // unwrap crash safety: all tokens exist in the vocabulary if it passes the above check
            .all(|query_token| {
                if let Some(posting_list) = &self.postings[query_token.unwrap() as usize] {
                    posting_list.contains(&point_id)
                } else {
                    false
                }
            })
    }
}

impl From<MutableInvertedIndex> for ImmutableInvertedIndex {
    fn from(index: MutableInvertedIndex) -> Self {
        ImmutableInvertedIndex {
            postings: index.postings,
            vocab: index.vocab,
            point_documents_tokens: index
                .point_to_docs
                .iter()
                .map(|doc| doc.as_ref().map(|doc| doc.len()))
                .collect(),
            points_count: index.points_count,
        }
    }
}
