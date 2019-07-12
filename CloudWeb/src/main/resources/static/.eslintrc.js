module.exports = {
    extends: ['eslint:recommended', 'plugin:react/recommended'],
    parserOptions: {
        ecmaFeatures: {
            jsx: true,
        },
        sourceType: 'module',
    },
    env: {
        browser: true,
        es6: true,
        node: true,
    },
    plugins: ['react'],
    rules: {
        indent: [2, 4],
        semi: [2, 'always'],
        'comma-dangle': 0,
        'space-before-function-paren': 0,
        camelcase: 0,
        'no-inner-declarations': 0,
        quotes: 0,
        'object-curly-spacing': 0,
        'no-global-assign': 0,
    },
};
