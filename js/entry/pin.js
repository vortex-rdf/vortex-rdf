// The chunk and batch iterables read their store: each holds it, so the store
// is not collected (and freed) while an iterable over it can still pull. Both
// entry points call this once, after `init()`.
export function pinStoresToIterables(VortexRdfStore) {
    for (const name of ['matchCodeChunks', 'matchCodesMany']) {
        const read = VortexRdfStore.prototype[name];
        VortexRdfStore.prototype[name] = function (...args) {
            const iterable = read.apply(this, args);
            if (iterable !== null) iterable._store = this;
            return iterable;
        };
    }
}
