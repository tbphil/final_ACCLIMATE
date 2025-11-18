module.exports = {
  root: true,
  env: { browser: true, node: true },
  parser: 'vue-eslint-parser',
  parserOptions: {
    parser: '@babel/eslint-parser',
    ecmaVersion: 2022,
    sourceType: 'module',
    requireConfigFile: false,
  },
  extends: [
    'eslint:recommended',
    'plugin:vue/vue3-recommended',
  ],
  settings: {
    'vue/setup-compiler-macros': true,
  },
  
  globals: {
    defineProps:  'readonly',
    defineEmits:  'readonly',
    defineExpose: 'readonly',
    withDefaults: 'readonly',
  },
  rules: {
    'vue/attribute-hyphenation': 'off',
    'vue/html-self-closing':   'off',
  },
};