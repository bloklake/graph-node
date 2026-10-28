-- Subgraph files kept by graph-node itself when it runs without an IPFS node.
create table if not exists public.ipfs_content (
    cid        text primary key,
    content    bytea not null,
    created_at timestamptz not null default now()
);
