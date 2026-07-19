//! BLAKE3 chunk-tree proof primitives for the v2 segment format.
//!
//! A sealed v2 segment commits to its content with the root of a canonical
//! left-balanced binary tree over fixed-size content chunks. Every hash is
//! domain-separated with `blake3` key derivation so a leaf can never be
//! confused with an interior node, an empty tree, or any other BLAKE3 use in
//! the workspace. The shape follows the bao/BLAKE3 split rule: the left
//! subtree covers the largest power of two strictly below the leaf count,
//! which keeps proofs logarithmic and the root reproducible from any stream.

/// Key-derivation context for hashing one content chunk into a leaf.
pub const CHUNK_TREE_LEAF_CONTEXT: &str = "vtop-engine 2026 segment-v2 chunk-tree leaf";
/// Key-derivation context for hashing `left || right` child digests.
pub const CHUNK_TREE_NODE_CONTEXT: &str = "vtop-engine 2026 segment-v2 chunk-tree node";
/// Key-derivation context for the root of an empty content region.
pub const CHUNK_TREE_EMPTY_CONTEXT: &str = "vtop-engine 2026 segment-v2 chunk-tree empty";

/// Chunking geometry a proof is verified against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkParams {
    pub chunk_size: u32,
    pub chunk_count: u64,
}

/// Which side of the join a proof sibling sits on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Left,
    Right,
}

/// Inclusion proof for one chunk: sibling digests ordered leaf-to-root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkProof {
    pub index: u64,
    pub path: Vec<(blake3::Hash, Side)>,
}

/// Hash one content chunk into a leaf digest.
pub fn leaf_hash(chunk: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new_derive_key(CHUNK_TREE_LEAF_CONTEXT);
    hasher.update(chunk);
    hasher.finalize()
}

/// Root digest of a segment with no content chunks.
pub fn empty_root() -> blake3::Hash {
    blake3::Hasher::new_derive_key(CHUNK_TREE_EMPTY_CONTEXT).finalize()
}

fn node_hash(left: &blake3::Hash, right: &blake3::Hash) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new_derive_key(CHUNK_TREE_NODE_CONTEXT);
    hasher.update(left.as_bytes());
    hasher.update(right.as_bytes());
    hasher.finalize()
}

/// Largest power of two strictly below `count`; the bao/BLAKE3 split rule.
fn left_subtree_leaves(count: u64) -> u64 {
    debug_assert!(count >= 2);
    1 << (u64::BITS - 1 - (count - 1).leading_zeros())
}

/// Root of the canonical tree over `leaves`; the empty root for no leaves.
pub fn tree_root(leaves: &[blake3::Hash]) -> blake3::Hash {
    match leaves {
        [] => empty_root(),
        [leaf] => *leaf,
        _ => {
            let split = left_subtree_leaves(leaves.len() as u64) as usize;
            node_hash(&tree_root(&leaves[..split]), &tree_root(&leaves[split..]))
        }
    }
}

/// Incrementally split a byte stream into `chunk_size` chunks and hash each
/// chunk into a leaf. The final chunk may be short.
pub struct ChunkTreeBuilder {
    chunk_size: usize,
    pending: Vec<u8>,
    leaves: Vec<blake3::Hash>,
}

impl ChunkTreeBuilder {
    /// Create a builder for `chunk_size`-byte chunks. Panics on zero.
    pub fn new(chunk_size: u32) -> Self {
        assert!(chunk_size > 0, "chunk_size must be greater than zero");
        Self {
            chunk_size: chunk_size as usize,
            pending: Vec::new(),
            leaves: Vec::new(),
        }
    }

    pub fn update(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let wanted = self.chunk_size - self.pending.len();
            let taken = wanted.min(bytes.len());
            self.pending.extend_from_slice(&bytes[..taken]);
            bytes = &bytes[taken..];
            if self.pending.len() == self.chunk_size {
                self.leaves.push(leaf_hash(&self.pending));
                self.pending.clear();
            }
        }
    }

    /// Hash any final short chunk and return the leaves with their root.
    pub fn finalize(mut self) -> (Vec<blake3::Hash>, blake3::Hash) {
        if !self.pending.is_empty() {
            self.leaves.push(leaf_hash(&self.pending));
        }
        let root = tree_root(&self.leaves);
        (self.leaves, root)
    }
}

/// Build the inclusion proof for `index`. Panics if `index` is out of range.
pub fn prove_chunk(leaves: &[blake3::Hash], index: u64) -> ChunkProof {
    let position = usize::try_from(index).expect("chunk index fits in memory");
    assert!(position < leaves.len(), "chunk index out of range");
    let mut path = Vec::new();
    collect_path(leaves, position, &mut path);
    ChunkProof { index, path }
}

fn collect_path(leaves: &[blake3::Hash], index: usize, path: &mut Vec<(blake3::Hash, Side)>) {
    if leaves.len() < 2 {
        return;
    }
    let split = left_subtree_leaves(leaves.len() as u64) as usize;
    if index < split {
        collect_path(&leaves[..split], index, path);
        path.push((tree_root(&leaves[split..]), Side::Right));
    } else {
        collect_path(&leaves[split..], index - split, path);
        path.push((tree_root(&leaves[..split]), Side::Left));
    }
}

/// Path length the canonical tree forces on `index` within `chunk_count`.
fn expected_path_len(chunk_count: u64, mut index: u64) -> usize {
    let mut count = chunk_count;
    let mut depth = 0;
    while count > 1 {
        let split = left_subtree_leaves(count);
        depth += 1;
        if index < split {
            count = split;
        } else {
            index -= split;
            count -= split;
        }
    }
    depth
}

/// Check that `chunk_bytes` is the `index`-th chunk under `root`.
///
/// Rejects out-of-range indexes, chunk lengths the geometry cannot produce
/// (every non-final chunk is exactly `chunk_size` bytes; the final chunk is
/// short but never empty), paths of the wrong depth, and digest mismatches.
pub fn verify_chunk(
    root: &blake3::Hash,
    params: ChunkParams,
    index: u64,
    chunk_bytes: &[u8],
    proof: &ChunkProof,
) -> bool {
    if proof.index != index || index >= params.chunk_count || params.chunk_size == 0 {
        return false;
    }
    let length = chunk_bytes.len() as u64;
    let is_final = index + 1 == params.chunk_count;
    let valid_length = if is_final {
        length > 0 && length <= u64::from(params.chunk_size)
    } else {
        length == u64::from(params.chunk_size)
    };
    if !valid_length || proof.path.len() != expected_path_len(params.chunk_count, index) {
        return false;
    }
    let mut current = leaf_hash(chunk_bytes);
    for (sibling, side) in &proof.path {
        current = match side {
            Side::Left => node_hash(sibling, &current),
            Side::Right => node_hash(&current, sibling),
        };
    }
    // blake3::Hash equality is constant-time.
    current == *root
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK_SIZE: u32 = 4;

    /// `count` four-byte chunks; chunk `i` is the byte `i` repeated.
    fn fixed_chunks(count: usize) -> Vec<Vec<u8>> {
        (0..count).map(|index| vec![index as u8; 4]).collect()
    }

    fn fixed_leaves(count: usize) -> Vec<blake3::Hash> {
        fixed_chunks(count)
            .iter()
            .map(|chunk| leaf_hash(chunk))
            .collect()
    }

    /// Reference root using the incremental merge-stack formulation instead
    /// of the recursive split, pinning that both describe the same tree.
    fn stack_root(leaves: &[blake3::Hash]) -> blake3::Hash {
        if leaves.is_empty() {
            return empty_root();
        }
        let mut stack: Vec<blake3::Hash> = Vec::new();
        for (index, leaf) in leaves.iter().enumerate() {
            let mut current = *leaf;
            let mut merged = index + 1;
            while merged % 2 == 0 {
                current = node_hash(&stack.pop().expect("stack holds the left tree"), &current);
                merged /= 2;
            }
            stack.push(current);
        }
        let mut current = stack.pop().expect("at least one subtree");
        while let Some(left) = stack.pop() {
            current = node_hash(&left, &current);
        }
        current
    }

    #[test]
    fn domain_separated_digests_match_golden_vectors() {
        assert_eq!(
            leaf_hash(b"vtop chunk").to_hex().as_str(),
            "b64fa09b7491f9e784de450ba8e59d4c3945746bdf56d5169b8de00f0f78155b"
        );
        assert_eq!(
            node_hash(&leaf_hash(b"left"), &leaf_hash(b"right"))
                .to_hex()
                .as_str(),
            "b07c0b045d96d3790e2840395f40aa0ee4b43ae868dc904d7394e1efcdbef286"
        );
        assert_eq!(
            empty_root().to_hex().as_str(),
            "380dd479d72e81e536759be6eb9c74e8a3f206ec896d9d8a98548c04240bc744"
        );
        // The three contexts must never collide over identical input bytes.
        assert_ne!(leaf_hash(&[]), empty_root());
        assert_ne!(
            leaf_hash(&[0_u8; 64]),
            node_hash(
                &blake3::Hash::from_bytes([0; 32]),
                &blake3::Hash::from_bytes([0; 32])
            )
        );
    }

    #[test]
    fn small_tree_roots_match_golden_vectors_pinning_the_split_rule() {
        let golden = [
            "57e95c8f4fe53907d9727759198cd0ce6c97a60908ff2fc48b56a61ac1022669",
            "11054d531d89779cb57e646fe1e9ba9a0658df783f263096efb2d2b0e1466a2d",
            "c64caeaaef2d421587a665c26943c9814c54fa082ca985ffaad34fb36e9a2a04",
            "f2357f8966ccbb27e9343b49f2ef1a07f37cd3c2c5901a0a5db00fe96fd59945",
            "d064529004d061c031b1e187eb772b18bdd142943756b2c82406fb8028468447",
        ];
        for (index, expected) in golden.iter().enumerate() {
            let root = tree_root(&fixed_leaves(index + 1));
            assert_eq!(root.to_hex().as_str(), *expected, "count {}", index + 1);
        }
    }

    #[test]
    fn five_chunk_proof_matches_golden_path() {
        let proof = prove_chunk(&fixed_leaves(5), 2);
        let shape: Vec<(String, Side)> = proof
            .path
            .iter()
            .map(|(hash, side)| (hash.to_hex().to_string(), *side))
            .collect();
        assert_eq!(
            shape,
            vec![
                (
                    "ac79ce71d9357b2573baa9634956679ab9e3b9aad18a40a34f89a2bb77168606".to_owned(),
                    Side::Right
                ),
                (
                    "11054d531d89779cb57e646fe1e9ba9a0658df783f263096efb2d2b0e1466a2d".to_owned(),
                    Side::Left
                ),
                (
                    "0f1e78dfa66f2b25eacdf9e3cd4ea4672865659604141a617c1b54e5613898b4".to_owned(),
                    Side::Right
                ),
            ]
        );
    }

    #[test]
    fn recursive_root_matches_merge_stack_reference_for_all_small_trees() {
        for count in 1..=33 {
            let leaves = fixed_leaves(count);
            assert_eq!(tree_root(&leaves), stack_root(&leaves), "count {count}");
        }
    }

    #[test]
    fn every_chunk_of_every_small_tree_verifies_and_mutations_fail() {
        for count in 1..=33_usize {
            let chunks = fixed_chunks(count);
            let leaves = fixed_leaves(count);
            let root = tree_root(&leaves);
            let params = ChunkParams {
                chunk_size: CHUNK_SIZE,
                chunk_count: count as u64,
            };
            for index in 0..count as u64 {
                let chunk = &chunks[index as usize];
                let proof = prove_chunk(&leaves, index);
                assert!(verify_chunk(&root, params, index, chunk, &proof));

                let other = (index + 1) % count as u64;
                if other != index {
                    assert!(!verify_chunk(&root, params, other, chunk, &proof));
                }
                let wrong_root = tree_root(&fixed_leaves(count + 1));
                assert!(!verify_chunk(&wrong_root, params, index, chunk, &proof));
                if !proof.path.is_empty() {
                    let mut truncated = proof.clone();
                    truncated.path.pop();
                    assert!(!verify_chunk(&root, params, index, chunk, &truncated));
                }
                let mut flipped = chunk.clone();
                flipped[0] ^= 0xff;
                assert!(!verify_chunk(&root, params, index, &flipped, &proof));
            }
        }
    }

    #[test]
    fn verify_rejects_out_of_range_indexes_and_impossible_chunk_lengths() {
        let leaves = fixed_leaves(3);
        let root = tree_root(&leaves);
        let params = ChunkParams {
            chunk_size: CHUNK_SIZE,
            chunk_count: 3,
        };
        let proof = prove_chunk(&leaves, 2);
        assert!(!verify_chunk(&root, params, 3, &[2; 4], &proof));
        // A non-final chunk must be exactly chunk_size bytes.
        let head = prove_chunk(&leaves, 0);
        assert!(!verify_chunk(&root, params, 0, &[0; 3], &head));
        // The final chunk may be short but never empty.
        assert!(!verify_chunk(&root, params, 2, &[], &proof));
        let empty = ChunkParams {
            chunk_size: CHUNK_SIZE,
            chunk_count: 0,
        };
        assert!(!verify_chunk(
            &empty_root(),
            empty,
            0,
            &[],
            &ChunkProof {
                index: 0,
                path: Vec::new()
            }
        ));
    }

    #[test]
    fn short_final_chunks_hash_and_verify_like_any_other_leaf() {
        let chunks: [&[u8]; 3] = [&[0; 4], &[1; 4], b"xy"];
        let leaves: Vec<_> = chunks.iter().map(|chunk| leaf_hash(chunk)).collect();
        let root = tree_root(&leaves);
        let params = ChunkParams {
            chunk_size: CHUNK_SIZE,
            chunk_count: 3,
        };
        let proof = prove_chunk(&leaves, 2);
        assert!(verify_chunk(&root, params, 2, b"xy", &proof));
        assert!(!verify_chunk(&root, params, 2, b"xz", &proof));
    }

    #[test]
    fn incremental_builder_equals_hashing_pre_split_chunks() {
        // 4 full chunks plus a 3-byte tail, fed in irregular pieces.
        let stream: Vec<u8> = (0..19).collect();
        let mut builder = ChunkTreeBuilder::new(CHUNK_SIZE);
        for piece in [&stream[..1], &stream[1..6], &stream[6..6], &stream[6..19]] {
            builder.update(piece);
        }
        let (leaves, root) = builder.finalize();

        let expected: Vec<_> = stream.chunks(CHUNK_SIZE as usize).map(leaf_hash).collect();
        assert_eq!(leaves, expected);
        assert_eq!(root, tree_root(&expected));

        let (no_leaves, empty) = ChunkTreeBuilder::new(CHUNK_SIZE).finalize();
        assert!(no_leaves.is_empty());
        assert_eq!(empty, empty_root());
    }
}
