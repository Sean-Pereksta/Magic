import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.*;
import javax.swing.text.*;
import javax.swing.tree.*;
import javax.swing.table.*;
import java.awt.*;
import java.awt.event.*;
import java.awt.datatransfer.StringSelection;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.List;
import java.util.zip.*;
import java.util.regex.*;

public class BibleReaderApp extends JFrame {
    private static final long serialVersionUID = 1L;
    private static final File DATA_FILE = new File("bible_reader_data.ser");
    private static final File BACKUP_DIR = new File("backups");

    private static final String OFFICIAL_BSB_USFM_ZIP =
            "https://github.com/BSB-publishing/bsb2usfm/releases/latest/download/BSB_usfm.zip";

    private static final String MORPHGNT_ZIP =
            "https://zenodo.org/records/376200/files/morphgnt%2Fsblgnt-6.12.zip?download=1";

    private static final String GUTENBERG_CATALOG_CSV =
            "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv";

    private final Color darkRed = new Color(92, 23, 23);
    private final Color cream = new Color(255, 253, 248);
    private final Color panelBg = new Color(248, 244, 236);
    private final Color noteYellow = new Color(255, 244, 150);
    private final Color categoryBlue = new Color(211, 233, 255);
    private final Color linkPurple = new Color(231, 218, 255);
    private final Color questionRed = new Color(255, 214, 214);
    private final Color discussionQuestionBlue = new Color(213, 226, 255);
    private final Color personalQuestionOrange = new Color(255, 229, 196);
    private final Color greekGreen = new Color(218, 245, 218);

    // Modern theme constants are centralized here so future UI polish can be made in one place.
    private final Color modernPrimaryRed = new Color(112, 28, 32);
    private final Color modernDarkRed = new Color(78, 18, 22);
    private final Color modernBackground = new Color(247, 244, 238);
    private final Color modernSurface = new Color(255, 252, 247);
    private final Color modernBorder = new Color(220, 207, 195);
    private final Color modernText = new Color(42, 35, 31);
    private final Color modernMutedText = new Color(105, 92, 84);
    private final Color modernSelection = new Color(236, 219, 207);
    private final Color modernDanger = new Color(154, 54, 58);
    private final Color modernDisabled = new Color(205, 196, 187);
    private final Font modernBaseFont = new Font("Segoe UI", Font.PLAIN, 14);
    private final Font modernBoldFont = new Font("Segoe UI", Font.BOLD, 14);
    private final Font modernHeaderFont = new Font("Segoe UI", Font.BOLD, 24);
    private final int modernGap = 10;
    private static final int RIGHT_SIDEBAR_PREFERRED_WIDTH = 360;
    private static final int RIGHT_SIDEBAR_MIN_WIDTH = 250;
    private static final int RIGHT_SIDEBAR_CONTENT_WIDTH = 320;
    private static final int RIGHT_SIDEBAR_SEARCH_HEIGHT = 300;
    private static final int RIGHT_SIDEBAR_SEARCH_MIN_HEIGHT = 235;
    private static final int STUDY_READER_MIN_WIDTH = 520;

    private AppData data;
    private Profile currentProfile;

    private CardLayout cards;
    private JPanel cardPanel;
    private JLabel statusLabel;
    private JLabel profileLabel;
    private JLabel sourceLabel;
    private JComboBox<String> profileBox;
    private JButton modernViewToggleButton;
    private final Map<String, JButton> navButtonsByCard = new HashMap<>();
    private String activeCardName = "study";
    private JComboBox<BookTreeItem> bookCombo;
    private JComboBox<Integer> chapterCombo;

    private DefaultMutableTreeNode rootNode;
    private DefaultTreeModel treeModel;
    private JTree libraryTree;
    private static final int LIBRARY_ROW_HEIGHT = 24;
    private boolean bibleTreeExpanded = false;

    private JSplitPane mainStudySplit;
    private JSplitPane centerRightSplit;
    private JPanel normalRightSidebar;
    private JButton exitReadingModeButton;
    private boolean readingMode = false;
    private int normalMainDividerLocation = -1;
    private int normalCenterRightDividerLocation = -1;
    private int normalReaderFontSize = 17;

    private JTextPane readerPane;
    private JPopupMenu selectionActionPopup;
    private Point readerSelectionPressPoint;
    private boolean readerSelectionDragged = false;
    private JPanel detailsPanel;
    private JPanel pinnedItemsPanel;
    private JPanel pinnedItemsBody;
    private JScrollPane pinnedItemsScroll;
    private JButton pinnedItemsToggleBtn;
    private boolean pinnedItemsExpanded = true;
    private JTextArea importLog;

    private JTextField memorySearchField;
    private JComboBox<String> memoryCategoryFilter;
    private DefaultListModel<MemoryVerse> memoryModel;
    private JList<MemoryVerse> memoryList;

    private JTextField searchField;
    private DefaultListModel<String> searchModel;
    private JList<String> searchList;

    private JTextField greekSearchField;
    private DefaultListModel<String> greekSearchModel;
    private JList<String> greekSearchList;
    private JTextPane greekSearchPreview;
    private JLabel greekSearchStatus;
    private String lastGreekSearchQuery = "";

    private DefaultListModel<String> categoryModel;
    private JList<String> categoryList;
    private JLabel selectedCategoryTitleLabel;
    private JPanel categoryResultsPanel;
    private JTextField categoryResultSearchField;
    private JScrollPane categoryResultsScroll;
    private final Set<String> expandedCategoryResultIds = new HashSet<>();

    private DefaultListModel<String> questionModel;
    private JList<String> questionList;
    private JPanel discussionQuestionsPanel;
    private JPanel personalQuestionsPanel;
    private StudyQuestion selectedQuestion;

    private DefaultListModel<TopicPage> topicPageModel;
    private JList<TopicPage> topicPageList;
    private JLabel topicTitleLabel;
    private JTextArea topicSummaryArea;
    private DefaultListModel<LinkedItem> topicLinkModel;
    private JList<LinkedItem> topicLinkList;

    private DefaultListModel<StudyProject> studyProjectModel;
    private JList<StudyProject> studyProjectList;
    private JPanel studyProjectDetailsPanel;
    private JTextField studyProjectSearchField;
    private JTextField allNotesSearchField;
    private DefaultListModel<StudySearchResult> studyProjectSearchModel;
    private JList<StudySearchResult> studyProjectSearchList;
    private DefaultListModel<StudySearchResult> allNotesSearchModel;
    private JList<StudySearchResult> allNotesSearchList;

    private JTextField recentSearchField;
    private JTextField categorySearchField;
    private JTextField questionSearchField;
    private JTextField topicPageSearchField;
    private JTextField bookmarkSearchField;
    private JTextField goToReferenceField;
    private JPopupMenu referenceSuggestionPopup;
    private JList<String> referenceSuggestionList;
    private DefaultListModel<String> referenceSuggestionModel;
    private JScrollPane readerScrollPane;
    private boolean marginNotesMode = false;
    private String rightSidebarMode = "margin";
    private CardLayout rightSidebarCards;
    private JPanel rightSidebarCardPanel;
    private final Map<String, JButton> rightSidebarToggleButtons = new HashMap<>();
    private JPanel marginNotesPanel;
    private JPanel marginNotesBody;
    private JScrollPane marginNotesScroll;
    private JButton marginNotesToggleButton;
    private final Set<String> expandedMarginNoteIds = new HashSet<>();
    private DefaultListModel<RecentLocation> recentlyOpenedModel;
    private JList<RecentLocation> recentlyOpenedList;
    private JPanel recentlyOpenedBody;
    private JButton recentlyOpenedToggleBtn;
    private boolean recentlyOpenedExpanded = true;
    private final java.util.List<NavigationLocation> backHistory = new ArrayList<>();
    private final java.util.List<NavigationLocation> forwardHistory = new ArrayList<>();
    private boolean restoringHistory = false;
    private JButton backButton;
    private JButton forwardButton;
    private JComboBox<String> recentFilterBox;
    private DefaultListModel<RecentAnnotationListItem> recentModel;
    private JList<RecentAnnotationListItem> recentList;
    private JTextField chapterNotesSearchField;
    private DefaultListModel<ChapterNote> chapterNotesModel;
    private JList<ChapterNote> chapterNotesList;

    private JPanel sideSearchPanel;
    private JPanel sideSearchBody;
    private JButton sideSearchToggleBtn;
    private JTextField sideSearchField;
    private DefaultListModel<String> sideSearchModel;
    private JList<String> sideSearchList;
    private JTextArea sideSearchPreview;
    private boolean sideSearchExpanded = true;

    private String selectedBook = "";
    private int selectedChapter = 1;
    private String currentSourceKey = "";
    private String currentSourceTitle = "";
    private boolean refreshingUi = false;
    private boolean loadingReader = false;
    private final java.util.List<AnnotationBubbleMarker> visibleAnnotationBubbles = new ArrayList<>();
    private final Map<String, JPopupMenu> openAnnotationPopups = new HashMap<>();

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (Exception ignored) {}

            new BibleReaderApp().setVisible(true);
        });
    }

    public BibleReaderApp() {
        super("Bible Study Library - Phrase Notes");
        data = loadData();
        repairLoadedData();

        if (data.profiles.isEmpty()) {
            data.profiles.put("Default Study", new Profile("Default Study"));
        }

        currentProfile = data.profiles.values().iterator().next();
        buildUi();
        refreshEverything();

        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(1450, 880);
        initializeStudySplitDividers();
        setLocationRelativeTo(null);
    }

    private void buildUi() {
        setLayout(new BorderLayout());
        getContentPane().setBackground(panelBg);

        JPanel top = new JPanel(new BorderLayout(6, 8));
        top.putClientProperty("modernHeader", Boolean.TRUE);
        top.setBackground(darkRed);
        top.setBorder(new EmptyBorder(10, 12, 10, 12));

        JPanel titlePanel = new JPanel(new GridLayout(2, 1));
        titlePanel.setOpaque(false);
        titlePanel.setPreferredSize(new Dimension(250, 52));
        titlePanel.setMinimumSize(new Dimension(210, 52));

        JLabel title = new JLabel("Bible Study Library");
        title.setForeground(Color.WHITE);
        title.setFont(new Font("Segoe UI", Font.BOLD, 25));

        profileLabel = new JLabel(" ");
        profileLabel.setForeground(new Color(255, 230, 230));
        profileLabel.setFont(new Font("Segoe UI", Font.PLAIN, 13));

        titlePanel.add(title);
        titlePanel.add(profileLabel);

        JPanel nav = new JPanel();
        nav.setOpaque(false);
        nav.setLayout(new BoxLayout(nav, BoxLayout.X_AXIS));

        profileBox = new JComboBox<>();
        profileBox.setPreferredSize(new Dimension(190, 34));
        profileBox.setMinimumSize(new Dimension(160, 34));
        profileBox.setMaximumSize(new Dimension(220, 34));
        profileBox.addActionListener(e -> switchProfile());

        JButton newProfile = navButton("New Profile");
        JButton study = navButton("Study");
        JButton importBtn = navButton("Import");
        JButton search = navButton("Search");
        JButton greekSearch = navButton("Greek Search");
        JButton memory = navButton("Memory Verses");
        JButton studyProjects = navButton("Study Projects");
        JButton recent = navButton("Notes Page");
        JButton categories = navButton("Categories");
        JButton questions = navButton("Questions");
        JButton topicPages = navButton("Topic Pages");
        JButton backup = navButton("Backup");
        JButton export = navButton("Export");
        modernViewToggleButton = navButton("Modern View: On");

        navButtonsByCard.clear();
        navButtonsByCard.put("study", study);
        navButtonsByCard.put("import", importBtn);
        navButtonsByCard.put("search", search);
        navButtonsByCard.put("greekSearch", greekSearch);
        navButtonsByCard.put("memory", memory);
        navButtonsByCard.put("studyProjects", studyProjects);
        navButtonsByCard.put("recent", recent);
        navButtonsByCard.put("categories", categories);
        navButtonsByCard.put("questions", questions);
        navButtonsByCard.put("topicPages", topicPages);

        newProfile.setToolTipText("Create a separate study profile.");
        study.setToolTipText("Study (Ctrl+1)");
        search.setToolTipText("Search (Ctrl+2)");
        greekSearch.setToolTipText("Greek Search (Ctrl+3)");
        memory.setToolTipText("Memory Verses (Ctrl+4)");
        studyProjects.setToolTipText("Study Projects (Ctrl+5)");
        recent.setToolTipText("Notes Page (Ctrl+6)");
        categories.setToolTipText("Categories (Ctrl+7)");
        questions.setToolTipText("Questions (Ctrl+8)");
        topicPages.setToolTipText("Topic Pages (Ctrl+9)");
        importBtn.setToolTipText("Import Bible, Greek, and library data.");
        backup.setToolTipText("Backup saved data.");
        export.setToolTipText("Export notes and study data.");
        greekSearch.setToolTipText("Greek Search (Ctrl+3) — search imported MorphGNT Greek text and morphology details.");
        backup.setToolTipText("Create a timestamped backup of your saved Bible study data.");
        export.setToolTipText("Export notes, questions, and memory verses to text.");
        modernViewToggleButton.setToolTipText("Switch between the modern polished view and the classic red/cream view.");

        newProfile.addActionListener(e -> createProfile());
        study.addActionListener(e -> showCard("study"));
        importBtn.addActionListener(e -> showCard("import"));
        search.addActionListener(e -> showCard("search"));
        greekSearch.addActionListener(e -> showCard("greekSearch"));
        memory.addActionListener(e -> { refreshMemoryVerses(); showCard("memory"); });
        studyProjects.addActionListener(e -> { refreshStudyProjects(); showCard("studyProjects"); });
        recent.addActionListener(e -> { refreshRecentNotes(); showCard("recent"); });
        categories.addActionListener(e -> { refreshCategories(); showCard("categories"); });
        questions.addActionListener(e -> { refreshQuestions(); showCard("questions"); });
        topicPages.addActionListener(e -> { refreshTopicPages(); showCard("topicPages"); });
        backup.addActionListener(e -> backupNow());
        export.addActionListener(e -> exportNotes());
        modernViewToggleButton.addActionListener(e -> toggleModernView());

        JPanel profileGroup = createNavGroup(labelWhite("Profile:"), profileBox, newProfile, modernViewToggleButton);
        JPanel studyGroup = createNavGroup(study, importBtn, search, greekSearch);
        JPanel memoryGroup = createNavGroup(memory, studyProjects, recent);
        JPanel notesGroup = createNavGroup(categories, questions, topicPages);
        JPanel dataGroup = createNavGroup(backup, export);

        nav.add(profileGroup);
        nav.add(Box.createHorizontalStrut(8));
        nav.add(studyGroup);
        nav.add(Box.createHorizontalStrut(8));
        nav.add(memoryGroup);
        nav.add(Box.createHorizontalStrut(8));
        nav.add(notesGroup);
        nav.add(Box.createHorizontalStrut(8));
        nav.add(dataGroup);

        JScrollPane navScroll = new JScrollPane(nav, JScrollPane.VERTICAL_SCROLLBAR_NEVER, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
        navScroll.setBorder(null);
        navScroll.setOpaque(false);
        navScroll.getViewport().setOpaque(false);
        navScroll.getHorizontalScrollBar().setUnitIncrement(24);
        navScroll.setPreferredSize(new Dimension(1180, 58));
        navScroll.setMinimumSize(new Dimension(720, 58));

        top.add(titlePanel, BorderLayout.WEST);
        top.add(navScroll, BorderLayout.CENTER);
        add(top, BorderLayout.NORTH);

        cards = new CardLayout();
        cardPanel = new JPanel(cards);
        cardPanel.add(buildStudyPage(), "study");
        cardPanel.add(buildImportPage(), "import");
        cardPanel.add(buildSearchPage(), "search");
        cardPanel.add(buildGreekSearchPage(), "greekSearch");
        cardPanel.add(buildMemoryPage(), "memory");
        cardPanel.add(buildStudyProjectsPage(), "studyProjects");
        cardPanel.add(buildRecentPage(), "recent");
        cardPanel.add(buildCategoriesPage(), "categories");
        cardPanel.add(buildQuestionsPage(), "questions");
        cardPanel.add(buildTopicPagesPage(), "topicPages");
        add(cardPanel, BorderLayout.CENTER);

        statusLabel = new JLabel(" Ready");
        statusLabel.setBorder(new EmptyBorder(7, 12, 7, 12));
        add(statusLabel, BorderLayout.SOUTH);
        installGlobalShortcuts();
        applyModernTheme(this);
        updateModernToggleText();
        updateActiveNavButton();
    }

    private void installGlobalShortcuts() {
        JRootPane root = getRootPane();
        InputMap input = root.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
        ActionMap actions = root.getActionMap();
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_LEFT, InputEvent.CTRL_DOWN_MASK), "globalPreviousChapter", this::previousChapter);
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_RIGHT, InputEvent.CTRL_DOWN_MASK), "globalNextChapter", this::nextChapter);
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_LEFT, InputEvent.ALT_DOWN_MASK), "globalBackLocation", this::goBackLocation);
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_RIGHT, InputEvent.ALT_DOWN_MASK), "globalForwardLocation", this::goForwardLocation);
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_K, InputEvent.CTRL_DOWN_MASK), "globalCommandPalette", this::showCommandPalette);
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_1, InputEvent.CTRL_DOWN_MASK), "globalStudy", () -> showCard("study"));
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_2, InputEvent.CTRL_DOWN_MASK), "globalSearch", () -> showCard("search"));
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_3, InputEvent.CTRL_DOWN_MASK), "globalGreekSearch", () -> showCard("greekSearch"));
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_4, InputEvent.CTRL_DOWN_MASK), "globalMemory", () -> { refreshMemoryVerses(); showCard("memory"); });
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_5, InputEvent.CTRL_DOWN_MASK), "globalStudyProjects", () -> { refreshStudyProjects(); showCard("studyProjects"); });
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_6, InputEvent.CTRL_DOWN_MASK), "globalRecent", () -> { refreshRecentNotes(); showCard("recent"); });
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_7, InputEvent.CTRL_DOWN_MASK), "globalCategories", () -> { refreshCategories(); showCard("categories"); });
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_8, InputEvent.CTRL_DOWN_MASK), "globalQuestions", () -> { refreshQuestions(); showCard("questions"); });
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_9, InputEvent.CTRL_DOWN_MASK), "globalTopics", () -> { refreshTopicPages(); showCard("topicPages"); });
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_B, InputEvent.CTRL_DOWN_MASK), "globalBookmarks", this::showBookmarksDialog);
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_F, InputEvent.CTRL_DOWN_MASK), "globalFind", this::focusBestSearchField);
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_L, InputEvent.CTRL_DOWN_MASK), "globalBookSelector", () -> { showCard("study"); if (bookCombo != null) bookCombo.requestFocusInWindow(); });
        bindShortcut(input, actions, KeyStroke.getKeyStroke(KeyEvent.VK_F11, 0), "globalReadingMode", this::toggleReadingMode);
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "globalExitReadingMode");
        actions.put("globalExitReadingMode", new AbstractAction() {
            public void actionPerformed(ActionEvent e) { if (readingMode) exitReadingMode(); }
        });
    }

    private void bindShortcut(InputMap input, ActionMap actions, KeyStroke key, String name, Runnable task) {
        input.put(key, name);
        actions.put(name, new AbstractAction() { public void actionPerformed(ActionEvent e) { task.run(); }});
    }

    private JPanel buildStudyPage() {
        JPanel page = new JPanel(new BorderLayout());
        page.setBackground(panelBg);

        mainStudySplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        mainStudySplit.setResizeWeight(0.22);
        mainStudySplit.setDividerSize(7);
        JPanel libraryPanel = buildLibraryPanel();
        libraryPanel.setMinimumSize(new Dimension(210, 10));
        mainStudySplit.setLeftComponent(libraryPanel);

        centerRightSplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
        centerRightSplit.setResizeWeight(1.0);
        centerRightSplit.setDividerSize(7);
        JPanel readerPanel = buildReaderPanel();
        readerPanel.setMinimumSize(new Dimension(STUDY_READER_MIN_WIDTH, 10));
        centerRightSplit.setLeftComponent(readerPanel);
        JPanel rightSidebar = buildRightSidebar();
        normalRightSidebar = rightSidebar;
        centerRightSplit.setRightComponent(rightSidebar);

        mainStudySplit.setRightComponent(centerRightSplit);
        page.add(mainStudySplit, BorderLayout.CENTER);
        return page;
    }

    private void initializeStudySplitDividers() {
        SwingUtilities.invokeLater(() -> {
            if (mainStudySplit != null && mainStudySplit.getWidth() > 0) {
                mainStudySplit.setDividerLocation(Math.max(240, Math.min(320, mainStudySplit.getWidth() / 4)));
            }
            clampCenterRightDivider(false);
            installStudySplitResizeClamp();
        });
    }

    private void installStudySplitResizeClamp() {
        if (centerRightSplit == null || Boolean.TRUE.equals(centerRightSplit.getClientProperty("sidebarResizeClampInstalled"))) return;
        centerRightSplit.putClientProperty("sidebarResizeClampInstalled", Boolean.TRUE);
        centerRightSplit.addComponentListener(new ComponentAdapter() {
            public void componentResized(ComponentEvent e) { clampCenterRightDivider(true); }
            public void componentShown(ComponentEvent e) { clampCenterRightDivider(true); }
        });
        addComponentListener(new ComponentAdapter() {
            public void componentResized(ComponentEvent e) { clampCenterRightDivider(true); }
        });
    }

    private void clampCenterRightDivider(boolean keepCurrentWhenValid) {
        if (centerRightSplit == null || centerRightSplit.getWidth() <= 0 || readingMode) return;
        int width = centerRightSplit.getWidth();
        int dividerSize = Math.max(0, centerRightSplit.getDividerSize());
        int maxDivider = Math.max(0, width - dividerSize - RIGHT_SIDEBAR_MIN_WIDTH);
        int minDivider = Math.min(STUDY_READER_MIN_WIDTH, maxDivider);
        int preferredDivider = Math.max(minDivider, width - dividerSize - RIGHT_SIDEBAR_PREFERRED_WIDTH);
        int current = centerRightSplit.getDividerLocation();
        int divider = keepCurrentWhenValid && current >= minDivider && current <= maxDivider ? current : preferredDivider;
        divider = Math.max(minDivider, Math.min(maxDivider, divider));
        if (current != divider) centerRightSplit.setDividerLocation(divider);
    }

    private JPanel buildLibraryPanel() {
        JPanel p = new JPanel(new BorderLayout(8, 8));
        p.setBorder(new EmptyBorder(10, 10, 10, 10));
        p.setBackground(panelBg);
        styleModernCard(p);

        JLabel h = new JLabel("Library");
        h.setFont(new Font("Segoe UI", Font.BOLD, 20));
        h.setForeground(darkRed);

        JPanel header = new JPanel(new BorderLayout(6, 6));
        header.setOpaque(false);
        JPanel treeControls = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 0));
        treeControls.setOpaque(false);
        JButton collapseBible = blackButton("Collapse Bible");
        JButton expandBible = blackButton("Expand Bible");
        collapseBible.addActionListener(e -> collapseBibleTree());
        expandBible.addActionListener(e -> expandBibleTree());
        treeControls.add(collapseBible);
        treeControls.add(expandBible);
        header.add(h, BorderLayout.NORTH);
        header.add(treeControls, BorderLayout.CENTER);

        rootNode = new DefaultMutableTreeNode("Library");
        treeModel = new DefaultTreeModel(rootNode);
        libraryTree = new JTree(treeModel);
        ToolTipManager.sharedInstance().registerComponent(libraryTree);
        libraryTree.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        libraryTree.setRowHeight(LIBRARY_ROW_HEIGHT);
        libraryTree.setShowsRootHandles(true);
        libraryTree.setRootVisible(true);
        libraryTree.setToggleClickCount(1);
        libraryTree.setCellRenderer(new LibraryTreeCellRenderer());
        libraryTree.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) { maybeOpenLibraryRow(e); }
            public void mousePressed(MouseEvent e) { maybeShowLibraryContextMenu(e); }
            public void mouseReleased(MouseEvent e) { maybeShowLibraryContextMenu(e); }
        });

        JScrollPane libraryScroll = new JScrollPane(libraryTree);
        libraryScroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        libraryScroll.setMinimumSize(new Dimension(0, 0));

        JPanel libraryStack = new JPanel(new GridBagLayout());
        libraryStack.setOpaque(false);
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = 0;
        gbc.fill = GridBagConstraints.BOTH;
        gbc.weightx = 1.0;
        gbc.insets = new Insets(0, 0, 6, 0);
        gbc.gridy = 0;
        gbc.weighty = 0.8;
        libraryStack.add(libraryScroll, gbc);

        gbc.gridy = 1;
        gbc.weighty = 0.2;
        gbc.insets = new Insets(0, 0, 0, 0);
        JPanel recentlyOpenedPanel = buildRecentlyOpenedPanel();
        recentlyOpenedPanel.setMinimumSize(new Dimension(0, 0));
        libraryStack.add(recentlyOpenedPanel, gbc);

        p.add(header, BorderLayout.NORTH);
        p.add(libraryStack, BorderLayout.CENTER);
        return p;
    }

    private JPanel buildReaderPanel() {
        JPanel p = new JPanel(new BorderLayout(8, 8));
        p.setBorder(new EmptyBorder(10, 10, 10, 10));
        p.setBackground(cream);
        styleModernCard(p);

        JPanel nav = new JPanel();
        nav.setLayout(new BoxLayout(nav, BoxLayout.Y_AXIS));
        nav.setOpaque(false);

        JPanel readerLocationControls = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
        readerLocationControls.setOpaque(false);

        JPanel readerActionControls = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        readerActionControls.setOpaque(false);

        bookCombo = new JComboBox<>();
        bookCombo.setPreferredSize(new Dimension(180, 30));

        chapterCombo = new JComboBox<>();
        chapterCombo.setPreferredSize(new Dimension(90, 30));

        sourceLabel = new JLabel("No Bible imported yet");
        sourceLabel.setForeground(new Color(100, 70, 55));

        exitReadingModeButton = blackButton("Exit Reading Mode");
        exitReadingModeButton.setToolTipText("Return sidebars and chapter controls after focused reading mode.");
        exitReadingModeButton.setVisible(false);
        exitReadingModeButton.addActionListener(e -> exitReadingMode());

        backButton = blackButton("← Back");
        backButton.setToolTipText("Go back to the previous reader location (Alt+Left).");
        backButton.addActionListener(e -> goBackLocation());

        forwardButton = blackButton("Forward →");
        forwardButton.setToolTipText("Go forward to the next reader location (Alt+Right).");
        forwardButton.addActionListener(e -> goForwardLocation());

        JButton previousChapter = blackButton("← Prev");
        previousChapter.setToolTipText("Previous Chapter (Ctrl+Left)");
        previousChapter.addActionListener(e -> previousChapter());

        JButton nextChapter = blackButton("Next →");
        nextChapter.setToolTipText("Next Chapter (Ctrl+Right)");
        nextChapter.addActionListener(e -> nextChapter());

        JButton bookmarkButton = blackButton("🔖 Bookmark");
        bookmarkButton.setToolTipText("Save your current Bible or library reading position.");
        bookmarkButton.addActionListener(e -> addBookmarkFromCurrentCaret(true));

        JButton bookmarksButton = blackButton("Bookmarks");
        bookmarksButton.setToolTipText("Bookmarks (Ctrl+B) — open, organize, or delete saved bookmarks.");
        bookmarksButton.addActionListener(e -> showBookmarksDialog());

        JButton bibleBookmarkButton = blackButton("Bible Bookmark");
        bibleBookmarkButton.setToolTipText("Jump to your most recent Bible bookmark.");
        bibleBookmarkButton.addActionListener(e -> goToBibleBookmark());

        JButton readingModeButton = blackButton("Reading Mode");
        readingModeButton.setToolTipText("Reading Mode (F11) — focus on the reader by temporarily hiding side panels.");
        readingModeButton.addActionListener(e -> enterReadingMode());

        marginNotesToggleButton = blackButton("Margin Notes");
        marginNotesToggleButton.setToolTipText("Show notes and questions in a margin beside the current reader source.");
        marginNotesToggleButton.addActionListener(e -> toggleMarginNotesMode());

        bookCombo.addActionListener(e -> {
            if (refreshingUi) return;
            Object o = bookCombo.getSelectedItem();
            if (o != null) {
                selectedBook = bookKeyFromComboItem(o);
                refreshChapterCombo();
                showSelectedChapter(true);
            }
        });

        chapterCombo.addActionListener(e -> {
            if (refreshingUi) return;
            Object o = chapterCombo.getSelectedItem();
            if (o instanceof Integer) {
                selectedChapter = (Integer) o;
                showSelectedChapter(true);
            }
        });

        readerLocationControls.add(exitReadingModeButton);
        readerLocationControls.add(new JLabel("Book:"));
        readerLocationControls.add(bookCombo);
        readerLocationControls.add(new JLabel("Chapter:"));
        readerLocationControls.add(chapterCombo);
        JButton chapterNotesButton = blackButton("✎");
        chapterNotesButton.setMargin(new Insets(2, 8, 2, 8));
        chapterNotesButton.setToolTipText("Edit notes for this chapter");
        chapterNotesButton.addActionListener(e -> openCurrentChapterNoteEditor());
        readerLocationControls.add(chapterNotesButton);
        readerLocationControls.add(backButton);
        readerLocationControls.add(forwardButton);

        readerActionControls.add(readingModeButton);
        readerActionControls.add(marginNotesToggleButton);
        readerActionControls.add(previousChapter);
        readerActionControls.add(nextChapter);
        readerActionControls.add(bookmarkButton);
        readerActionControls.add(bookmarksButton);
        readerActionControls.add(bibleBookmarkButton);
        readerActionControls.add(sourceLabel);

        nav.add(readerLocationControls);
        nav.add(readerActionControls);

        JPanel goPanel = new JPanel(new BorderLayout(6, 0));
        goPanel.setOpaque(false);
        goToReferenceField = new JTextField();
        goToReferenceField.setToolTipText("Go to Reference — type Romans 14, Romans 14:13, Gen 1, or John 3:16.");
        installReferenceAutocomplete();
        JButton goReferenceButton = blackButton("Go");
        goReferenceButton.setToolTipText("Open the typed Bible reference.");
        goReferenceButton.addActionListener(e -> goToReferenceFromBox());
        goPanel.add(new JLabel("Go to Reference:"), BorderLayout.WEST);
        goPanel.add(goToReferenceField, BorderLayout.CENTER);
        goPanel.add(goReferenceButton, BorderLayout.EAST);

        JPanel topPanel = new JPanel(new BorderLayout(6, 6));
        topPanel.setOpaque(false);
        topPanel.add(nav, BorderLayout.NORTH);
        topPanel.add(goPanel, BorderLayout.SOUTH);

        readerPane = new JTextPane() {
            public String getToolTipText(MouseEvent e) {
                int pos = viewToModel2D(e.getPoint());
                AnnotationBubbleMarker bubble = bubbleAt(pos);
                TextAnnotation a = bubble == null ? annotationAt(pos) : bubble.annotation;
                if (a == null) return null;
                return "<html><b>" + esc(a.type) + "</b>" +
                        (a.category.isEmpty() ? "" : "<br>Category: " + esc(a.category)) +
                        (a.target.isEmpty() ? "" : "<br>Attached: " + esc(a.target)) +
                        "<br>" + esc(shorten(a.note, 240)).replace("\n", "<br>") + "</html>";
            }
        };

        ToolTipManager.sharedInstance().registerComponent(readerPane);
        readerPane.setEditable(false);
        readerPane.setFont(new Font("Georgia", Font.PLAIN, 17));
        readerPane.setMargin(new Insets(14, 16, 14, 16));
        readerPane.setBackground(cream);
        readerPane.addMouseListener(new MouseAdapter() {
            public void mousePressed(MouseEvent e) {
                hideSelectionActionPopup();
                if (SwingUtilities.isLeftMouseButton(e)) {
                    readerSelectionPressPoint = e.getPoint();
                    readerSelectionDragged = false;
                }
                if (e.isPopupTrigger()) showReaderMenu(e);
            }
            public void mouseReleased(MouseEvent e) {
                if (e.isPopupTrigger()) {
                    hideSelectionActionPopup();
                    showReaderMenu(e);
                    return;
                }
                if (SwingUtilities.isLeftMouseButton(e) && readerSelectionDragged && e.getClickCount() == 1) {
                    showSelectionActionPopupIfNeeded();
                }
            }
            public void mouseClicked(MouseEvent e) {
                if (!SwingUtilities.isLeftMouseButton(e)) return;
                boolean consumed = handleReaderLeftClick(e);
                if (!consumed && e.getClickCount() == 2) {
                    hideSelectionActionPopup();
                    showQuickNoteForSelectionOrWord(e);
                }
            }
        });
        readerPane.addMouseMotionListener(new MouseMotionAdapter() {
            public void mouseDragged(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && readerSelectionPressPoint != null
                        && readerSelectionPressPoint.distance(e.getPoint()) > 4) {
                    readerSelectionDragged = true;
                }
            }
        });
        readerPane.addCaretListener(e -> {
            if (readerPane.getSelectionEnd() <= readerPane.getSelectionStart()) hideSelectionActionPopup();
        });
        installReaderShortcuts();

        p.add(topPanel, BorderLayout.NORTH);
        readerScrollPane = new JScrollPane(readerPane);
        readerScrollPane.getVerticalScrollBar().addAdjustmentListener(e -> {
            if (!e.getValueIsAdjusting() && marginNotesMode) SwingUtilities.invokeLater(this::refreshMarginNotesPanel);
        });
        p.add(readerScrollPane, BorderLayout.CENTER);
        return p;
    }

    private JPanel buildRightSidebar() {
        // The old Selection Details panel is no longer displayed,
        // but older reader/detail methods still write to detailsPanel.
        // Keep an invisible backing panel so those methods do not crash.
        ensureDetailsPanel();

        JPanel wrapper = new JPanel(new BorderLayout(8, 8));
        wrapper.setBackground(panelBg);
        wrapper.setBorder(new EmptyBorder(6, 6, 6, 6));
        wrapper.setPreferredSize(new Dimension(RIGHT_SIDEBAR_PREFERRED_WIDTH, 10));
        wrapper.setMinimumSize(new Dimension(RIGHT_SIDEBAR_MIN_WIDTH, 10));

        JPanel header = new JPanel();
        header.setLayout(new BoxLayout(header, BoxLayout.Y_AXIS));
        header.setOpaque(false);

        JLabel title = new JLabel("Study Notes");
        title.setFont(new Font("Segoe UI", Font.BOLD, 16));
        title.setForeground(darkRed);
        title.setAlignmentX(Component.LEFT_ALIGNMENT);

        JPanel toggles = new JPanel(new GridLayout(0, 2, 4, 4));
        toggles.setOpaque(false);
        addRightSidebarToggle(toggles, "margin", "Margin Notes");
        addRightSidebarToggle(toggles, "search", "Quick Search");
        addRightSidebarToggle(toggles, "pinned", "Pinned Items");
        JButton wholeChapterNotes = blackButton("Whole Chapter Notes");
        wholeChapterNotes.setFont(new Font("Segoe UI", Font.BOLD, 11));
        wholeChapterNotes.setMargin(new Insets(3, 4, 3, 4));
        wholeChapterNotes.setToolTipText("Open notes for the current chapter.");
        wholeChapterNotes.addActionListener(e -> openCurrentChapterNoteEditor());
        toggles.add(wholeChapterNotes);
        toggles.setAlignmentX(Component.LEFT_ALIGNMENT);

        header.add(title);
        header.add(Box.createVerticalStrut(6));
        header.add(toggles);

        rightSidebarCards = new CardLayout();
        rightSidebarCardPanel = new JPanel(rightSidebarCards);
        rightSidebarCardPanel.setBackground(panelBg);
        rightSidebarCardPanel.add(buildMarginNotesPanel(), "margin");
        rightSidebarCardPanel.add(buildSideSearchPanel(), "search");
        rightSidebarCardPanel.add(buildPinnedItemsPanel(), "pinned");

        wrapper.add(header, BorderLayout.NORTH);
        wrapper.add(rightSidebarCardPanel, BorderLayout.CENTER);
        SwingUtilities.invokeLater(() -> showRightSidebarMode(rightSidebarMode));
        return wrapper;
    }

    private void addRightSidebarToggle(JPanel parent, String mode, String label) {
        JButton button = blackButton(label);
        button.setFont(new Font("Segoe UI", Font.BOLD, 11));
        button.setMargin(new Insets(3, 4, 3, 4));
        button.setToolTipText("Show " + label + " in the right sidebar.");
        button.addActionListener(e -> showRightSidebarMode(mode));
        rightSidebarToggleButtons.put(mode, button);
        parent.add(button);
    }

    private void showRightSidebarMode(String mode) {
        String normalized = safe(mode).isEmpty() ? "margin" : mode;
        if (!"margin".equals(normalized) && !"search".equals(normalized) && !"pinned".equals(normalized)) normalized = "margin";
        rightSidebarMode = normalized;
        marginNotesMode = "margin".equals(rightSidebarMode);
        if (rightSidebarCards != null && rightSidebarCardPanel != null) {
            rightSidebarCards.show(rightSidebarCardPanel, rightSidebarMode);
        }
        updateRightSidebarToggleButtons();
        if (marginNotesMode) refreshMarginNotesPanel();
        if ("pinned".equals(rightSidebarMode)) refreshPinnedItems();
        if (rightSidebarCardPanel != null) {
            rightSidebarCardPanel.revalidate();
            rightSidebarCardPanel.repaint();
        }
    }

    private void updateRightSidebarToggleButtons() {
        for (Map.Entry<String, JButton> entry : rightSidebarToggleButtons.entrySet()) {
            JButton button = entry.getValue();
            if (button == null) continue;
            boolean active = entry.getKey().equals(rightSidebarMode);
            button.setFont(new Font("Segoe UI", Font.BOLD, 11));
            button.setBackground(active ? modernPrimaryRed : modernDarkRed);
            button.setForeground(Color.WHITE);
            button.setBorder(new CompoundBorder(
                    new RoundedBorder(active ? modernPrimaryRed : modernBorder, 12, new Insets(1, 1, 1, 1)),
                    new EmptyBorder(3, 4, 3, 4)));
        }
    }

    private JPanel buildPinnedItemsPanel() {
        pinnedItemsPanel = new JPanel(new BorderLayout(6, 6));
        pinnedItemsPanel.setBackground(panelBg);
        pinnedItemsPanel.setBorder(new EmptyBorder(0, 0, 0, 0));

        JPanel header = new JPanel(new BorderLayout(6, 6));
        header.setOpaque(false);
        JLabel title = new JLabel("Pinned Study Items");
        title.setFont(new Font("Segoe UI", Font.BOLD, 14));
        title.setForeground(darkRed);
        JButton refresh = blackButton("Refresh");
        refresh.setMargin(new Insets(3, 7, 3, 7));
        refresh.addActionListener(e -> refreshPinnedItems());
        header.add(title, BorderLayout.WEST);
        header.add(refresh, BorderLayout.EAST);

        pinnedItemsBody = new WidthTrackingPanel();
        pinnedItemsBody.setLayout(new BoxLayout(pinnedItemsBody, BoxLayout.Y_AXIS));
        pinnedItemsBody.setBackground(cream);
        pinnedItemsBody.setBorder(new EmptyBorder(8, 8, 8, 8));

        pinnedItemsScroll = new JScrollPane(pinnedItemsBody);
        pinnedItemsScroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        pinnedItemsScroll.getVerticalScrollBar().setUnitIncrement(16);
        pinnedItemsScroll.setMinimumSize(new Dimension(0, 0));

        pinnedItemsPanel.add(header, BorderLayout.NORTH);
        pinnedItemsPanel.add(pinnedItemsScroll, BorderLayout.CENTER);
        return pinnedItemsPanel;
    }

    private JPanel ensureDetailsPanel() {
        if (detailsPanel == null) {
            detailsPanel = new WidthTrackingPanel();
            detailsPanel.setLayout(new BoxLayout(detailsPanel, BoxLayout.Y_AXIS));
            detailsPanel.setBackground(cream);
            detailsPanel.setBorder(new EmptyBorder(6, 8, 8, 8));
        }
        return detailsPanel;
    }

    private JPanel buildDetailsPanel() {
        JPanel p = new JPanel(new BorderLayout(4, 4));
        p.setMinimumSize(new Dimension(0, 90));
        p.setPreferredSize(new Dimension(RIGHT_SIDEBAR_CONTENT_WIDTH, 125));
        p.setBorder(new CompoundBorder(new EmptyBorder(6, 0, 0, 0), new RoundedBorder(modernBorder, 12, new Insets(1, 1, 1, 1))));
        p.setBackground(cream);

        JLabel h = new JLabel("Selection Details");
        h.setFont(new Font("Segoe UI", Font.BOLD, 12));
        h.setForeground(modernMutedText);
        h.setBorder(new EmptyBorder(6, 8, 0, 8));

        ensureDetailsPanel();

        p.add(h, BorderLayout.NORTH);
        JScrollPane detailsScroll = new JScrollPane(detailsPanel);
        detailsScroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        detailsScroll.setMinimumSize(new Dimension(0, 0));
        detailsScroll.setBorder(null);
        p.add(detailsScroll, BorderLayout.CENTER);
        return p;
    }

    private JPanel buildSideSearchPanel() {
        JPanel outer = new JPanel(new BorderLayout(6, 6));
        outer.setMinimumSize(new Dimension(0, 0));
        outer.setBorder(new EmptyBorder(0, 0, 0, 0));
        outer.setBackground(panelBg);

        JPanel header = new JPanel(new BorderLayout(6, 6));
        header.setOpaque(false);

        JLabel title = new JLabel("Quick Search");
        title.setFont(new Font("Segoe UI", Font.BOLD, 14));
        title.setForeground(darkRed);
        JLabel hint = new JLabel("Search verses, books, highlights, and questions");
        hint.setFont(new Font("Segoe UI", Font.PLAIN, 11));
        hint.setForeground(modernMutedText);
        JPanel titleStack = new JPanel();
        titleStack.setLayout(new BoxLayout(titleStack, BoxLayout.Y_AXIS));
        titleStack.setOpaque(false);
        titleStack.add(title);
        titleStack.add(hint);
        header.add(titleStack, BorderLayout.WEST);

        sideSearchBody = new JPanel(new BorderLayout(6, 6));
        sideSearchBody.setMinimumSize(new Dimension(0, 0));
        sideSearchBody.setOpaque(false);

        JPanel inputRow = new JPanel(new BorderLayout(5, 5));
        inputRow.setOpaque(false);

        sideSearchField = new JTextField();
        sideSearchField.setToolTipText("Quick Search (Ctrl+F)");
        sideSearchField.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        sideSearchField.addActionListener(e -> doSideSearch());

        JButton go = blackButton("Go");
        go.addActionListener(e -> doSideSearch());

        inputRow.add(sideSearchField, BorderLayout.CENTER);
        inputRow.add(go, BorderLayout.EAST);

        sideSearchModel = new DefaultListModel<>();
        sideSearchList = new JList<>(sideSearchModel);
        sideSearchList.setFont(new Font("Consolas", Font.PLAIN, 12));
        sideSearchList.setVisibleRowCount(8);
        sideSearchList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isRightMouseButton(e)) {
                    int idx = sideSearchList.locationToIndex(e.getPoint());
                    if (idx >= 0) sideSearchList.setSelectedIndex(idx);
                    showSideSearchMenu(e);
                    return;
                }

                if (SwingUtilities.isLeftMouseButton(e)) {
                    previewSideSearchResult();
                    if (e.getClickCount() == 2) showFullViewForSideSearchResult();
                }
            }
            public void mousePressed(MouseEvent e) { if (e.isPopupTrigger()) showSideSearchMenu(e); }
            public void mouseReleased(MouseEvent e) { if (e.isPopupTrigger()) showSideSearchMenu(e); }
        });

        sideSearchPreview = readonlyArea();
        sideSearchPreview.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        sideSearchPreview.setText("Search Bible verses, Greek entries, imported books, highlights, and questions here without leaving the study screen. Click a result to preview it. Right-click for full view.");

        JScrollPane sideSearchListScroll = new JScrollPane(sideSearchList);
        JScrollPane sideSearchPreviewScroll = new JScrollPane(sideSearchPreview);
        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, sideSearchListScroll, sideSearchPreviewScroll);
        split.setResizeWeight(0.48);
        split.setDividerSize(5);
        split.setMinimumSize(new Dimension(0, 180));
        ((JScrollPane) split.getTopComponent()).setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        ((JScrollPane) split.getBottomComponent()).setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);

        sideSearchBody.add(inputRow, BorderLayout.NORTH);
        sideSearchBody.add(split, BorderLayout.CENTER);

        outer.add(header, BorderLayout.NORTH);
        outer.add(sideSearchBody, BorderLayout.CENTER);
        return outer;
    }


    private JPanel buildRecentlyOpenedPanel() {
        JPanel outer = new JPanel(new BorderLayout(4, 4));
        outer.setMinimumSize(new Dimension(0, 0));
        outer.setBackground(panelBg);
        outer.setBorder(new CompoundBorder(new LineBorder(new Color(190, 160, 150)), new EmptyBorder(4, 4, 4, 4)));
        JPanel header = new JPanel(new BorderLayout(6, 6));
        header.setOpaque(false);
        JLabel title = new JLabel("Recently Opened");
        title.setFont(new Font("Segoe UI", Font.BOLD, 14));
        title.setForeground(darkRed);
        recentlyOpenedToggleBtn = blackButton("Minimize");
        recentlyOpenedToggleBtn.addActionListener(e -> toggleRecentlyOpened());
        header.add(title, BorderLayout.WEST);
        header.add(recentlyOpenedToggleBtn, BorderLayout.EAST);
        recentlyOpenedBody = new JPanel(new BorderLayout(4, 4));
        recentlyOpenedBody.setOpaque(false);
        recentlyOpenedModel = new DefaultListModel<>();
        recentlyOpenedList = new JList<>(recentlyOpenedModel);
        recentlyOpenedList.setVisibleRowCount(5);
        recentlyOpenedList.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        recentlyOpenedList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) openRecentlyOpenedSelection();
            }
        });
        JScrollPane recentlyOpenedScroll = new JScrollPane(recentlyOpenedList);
        recentlyOpenedScroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        recentlyOpenedScroll.setMinimumSize(new Dimension(0, 0));
        recentlyOpenedBody.add(recentlyOpenedScroll, BorderLayout.CENTER);
        outer.add(header, BorderLayout.NORTH);
        outer.add(recentlyOpenedBody, BorderLayout.CENTER);
        return outer;
    }

    private void toggleRecentlyOpened() {
        recentlyOpenedExpanded = !recentlyOpenedExpanded;
        if (recentlyOpenedBody != null) recentlyOpenedBody.setVisible(recentlyOpenedExpanded);
        if (recentlyOpenedToggleBtn != null) recentlyOpenedToggleBtn.setText(recentlyOpenedExpanded ? "Minimize" : "Show");
    }

    private JPanel buildStudyProjectsPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(10, 10, 10, 10));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Study Projects");
        h.setFont(new Font("Segoe UI", Font.BOLD, 22));
        h.setForeground(darkRed);
        page.add(h, BorderLayout.NORTH);

        studyProjectModel = new DefaultListModel<>();
        studyProjectList = new JList<>(studyProjectModel);
        studyProjectList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        studyProjectList.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        studyProjectList.addListSelectionListener(e -> { if (!e.getValueIsAdjusting()) renderSelectedStudyProject(); });

        JButton create = blackButton("Create Project");
        create.addActionListener(e -> createStudyProject());
        JButton edit = blackButton("Rename/Edit Project");
        edit.addActionListener(e -> editSelectedStudyProject());
        JButton delete = blackButton("Delete Project");
        delete.addActionListener(e -> deleteSelectedStudyProject());

        JPanel leftButtons = new JPanel(new GridLayout(0, 1, 6, 6));
        leftButtons.setOpaque(false);
        leftButtons.add(create);
        leftButtons.add(edit);
        leftButtons.add(delete);

        JPanel left = new JPanel(new BorderLayout(8, 8));
        left.setBackground(panelBg);
        left.setBorder(new CompoundBorder(new LineBorder(new Color(180, 145, 135)), new EmptyBorder(8, 8, 8, 8)));
        JTextField projectFilterField = new JTextField();
        projectFilterField.setToolTipText("Filter study projects...");
        projectFilterField.getDocument().addDocumentListener(new SimpleDocumentListener(() -> { studyProjectSearchField.putClientProperty("projectFilter", projectFilterField.getText()); refreshStudyProjects(); }));
        left.add(projectFilterField, BorderLayout.NORTH);
        left.add(new JScrollPane(studyProjectList), BorderLayout.CENTER);
        left.add(leftButtons, BorderLayout.SOUTH);

        studyProjectDetailsPanel = new JPanel();
        studyProjectDetailsPanel.setLayout(new BoxLayout(studyProjectDetailsPanel, BoxLayout.Y_AXIS));
        studyProjectDetailsPanel.setBackground(cream);
        studyProjectDetailsPanel.setBorder(new EmptyBorder(10, 10, 10, 10));

        studyProjectSearchField = new JTextField();
        studyProjectSearchField.setToolTipText("Search this study...");
        JButton searchStudy = blackButton("Search this study...");
        searchStudy.addActionListener(e -> searchSelectedStudyProject());
        studyProjectSearchModel = new DefaultListModel<>();
        studyProjectSearchList = new JList<>(studyProjectSearchModel);
        studyProjectSearchList.setVisibleRowCount(5);
        studyProjectSearchList.addMouseListener(new MouseAdapter() { public void mouseClicked(MouseEvent e) { if (e.getClickCount() == 2) openStudySearchResult(studyProjectSearchList.getSelectedValue()); }});

        allNotesSearchField = new JTextField();
        allNotesSearchField.setToolTipText("Search all notes...");
        JButton searchAll = blackButton("Search all notes...");
        searchAll.addActionListener(e -> searchAllStudyNotes());
        allNotesSearchModel = new DefaultListModel<>();
        allNotesSearchList = new JList<>(allNotesSearchModel);
        allNotesSearchList.setVisibleRowCount(5);
        allNotesSearchList.addMouseListener(new MouseAdapter() { public void mouseClicked(MouseEvent e) { if (e.getClickCount() == 2) openStudySearchResult(allNotesSearchList.getSelectedValue()); }});

        JPanel searchBars = new JPanel(new GridLayout(0, 1, 5, 5));
        searchBars.setOpaque(false);
        searchBars.add(studyProjectSearchField);
        searchBars.add(searchStudy);
        searchBars.add(new JScrollPane(studyProjectSearchList));
        searchBars.add(allNotesSearchField);
        searchBars.add(searchAll);
        searchBars.add(new JScrollPane(allNotesSearchList));

        JPanel right = new JPanel(new BorderLayout(8, 8));
        right.setBackground(panelBg);
        right.add(searchBars, BorderLayout.NORTH);
        right.add(new JScrollPane(studyProjectDetailsPanel), BorderLayout.CENTER);

        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, right);
        split.setResizeWeight(0.25);
        page.add(split, BorderLayout.CENTER);
        return page;
    }


    private JPanel buildImportPage() {
        JPanel page = new JPanel(new BorderLayout(12, 12));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Import Library Data");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JTextArea instructions = readonlyArea();
        instructions.setText(
                "Import the Bible and philosophy/public-domain works here.\n\n" +
                "Bible options:\n" +
                "• Download + Import Official BSB USFM\n" +
                "• Import BSB/USFM ZIP or Folder\n" +
                "• Import Bible CSV: Book,Chapter,Verse,Text\n\n" +
                "Greek options:\n" +
                "• Download + Import MorphGNT Greek\n" +
                "• Import MorphGNT ZIP / TXT Folder\n" +
                "• Import Greek CSV: Book,Chapter,Verse,GreekText,Details\n" +
                "After Greek is imported, right-click a verse number and choose 'Check Greek For This Verse'.\n\n" +
                "Philosophy / other works:\n" +
                "• Import TXT, then select exact words or paragraphs in the reader.\n" +
                "• Browse Project Gutenberg books, then import the selected public-domain plain text.\n" +
                "• Right-click selected text to add notes, categories, questions, or attachments.\n" +
                "• Attachments can point to a Bible verse like Romans 14:13 or to a unique chunk inside a selected book.\n\n" +
                "Hover over highlighted text to preview its note. Click highlighted text to see actions in the right panel."
        );

        JPanel actions = new JPanel(new GridLayout(0, 1, 10, 10));
        actions.setOpaque(false);
        actions.setPreferredSize(new Dimension(360, 1));

        JButton downloadBsb = blackButton("Download + Import Official BSB USFM");
        JButton importUsfm = blackButton("Import BSB/USFM ZIP or Folder");
        JButton importBibleCsv = blackButton("Import Bible CSV");
        JButton downloadGreek = blackButton("Download + Import MorphGNT Greek");
        JButton importGreekZip = blackButton("Import MorphGNT ZIP / TXT Folder");
        JButton importGreekCsv = blackButton("Import Greek CSV");
        JButton importTxt = blackButton("Import Philosophy / Public Domain TXT");
        JButton browseGutenberg = blackButton("Browse Project Gutenberg Books");
        JButton templates = blackButton("Create Import Templates");
        JButton clearBible = blackButton("Clear Bible Text Only");

        downloadBsb.addActionListener(e -> downloadAndImportBsbUsfm());
        importUsfm.addActionListener(e -> importUsfmZipOrFolder());
        importBibleCsv.addActionListener(e -> importBibleCsv());
        downloadGreek.addActionListener(e -> downloadAndImportMorphGnt());
        importGreekZip.addActionListener(e -> importMorphGntZipOrFolder());
        importGreekCsv.addActionListener(e -> importGreekCsv());
        importTxt.addActionListener(e -> importLibraryText());
        browseGutenberg.addActionListener(e -> browseProjectGutenbergBooksDialog());
        templates.addActionListener(e -> createTemplates());
        clearBible.addActionListener(e -> clearBibleText());

        actions.add(downloadBsb);
        actions.add(importUsfm);
        actions.add(importBibleCsv);
        actions.add(downloadGreek);
        actions.add(importGreekZip);
        actions.add(importGreekCsv);
        actions.add(importTxt);
        actions.add(browseGutenberg);
        actions.add(templates);
        actions.add(clearBible);

        importLog = new JTextArea();
        importLog.setEditable(false);
        importLog.setFont(new Font("Consolas", Font.PLAIN, 13));
        importLog.setLineWrap(true);
        importLog.setWrapStyleWord(true);
        importLog.setText("Import log will appear here.\n");

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(instructions), new JScrollPane(importLog));
        split.setResizeWeight(0.55);
        split.setDividerSize(7);

        page.add(h, BorderLayout.NORTH);
        page.add(split, BorderLayout.CENTER);
        page.add(actions, BorderLayout.EAST);
        return page;
    }


    private JPanel buildMemoryPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Memory Verses");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        memorySearchField = new JTextField();
        memorySearchField.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        memorySearchField.addActionListener(e -> refreshMemoryVerses());
        memorySearchField.getDocument().addDocumentListener(new SimpleDocumentListener(this::refreshMemoryVerses));

        memoryCategoryFilter = new JComboBox<>();
        memoryCategoryFilter.setPreferredSize(new Dimension(180, 30));
        memoryCategoryFilter.addActionListener(e -> refreshMemoryVerses());

        JButton clear = blackButton("Clear");
        clear.addActionListener(e -> {
            memorySearchField.setText("");
            memoryCategoryFilter.setSelectedItem("All Categories");
            refreshMemoryVerses();
        });

        JPanel filters = new JPanel(new BorderLayout(8, 8));
        filters.setOpaque(false);
        JPanel filterRight = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        filterRight.setOpaque(false);
        filterRight.add(new JLabel("Category:"));
        filterRight.add(memoryCategoryFilter);
        filterRight.add(clear);
        filters.add(memorySearchField, BorderLayout.CENTER);
        filters.add(filterRight, BorderLayout.EAST);

        JButton manualAdd = blackButton("Manual Add By Reference");
        JButton review = blackButton("Review");
        JButton edit = blackButton("Edit");
        JButton delete = blackButton("Delete");
        JButton flashcards = blackButton("Start Flashcards");

        manualAdd.addActionListener(e -> addMemoryVerseManually());
        review.addActionListener(e -> reviewSelectedMemoryVerse());
        edit.addActionListener(e -> editSelectedMemoryVerse());
        delete.addActionListener(e -> deleteSelectedMemoryVerse());
        flashcards.addActionListener(e -> createMemoryFlashcards());

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
        actions.setOpaque(false);
        actions.add(manualAdd);
        actions.add(review);
        actions.add(edit);
        actions.add(delete);
        actions.add(flashcards);

        JPanel north = new JPanel(new BorderLayout(8, 8));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);
        north.add(filters, BorderLayout.CENTER);
        north.add(actions, BorderLayout.SOUTH);

        memoryModel = new DefaultListModel<>();
        memoryList = new JList<>(memoryModel);
        memoryList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        memoryList.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        memoryList.setFixedCellHeight(118);
        memoryList.setCellRenderer(new MemoryVerseCellRenderer());
        memoryList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) reviewSelectedMemoryVerse();
            }
        });

        page.add(north, BorderLayout.NORTH);
        page.add(new JScrollPane(memoryList), BorderLayout.CENTER);
        return page;
    }

    private JPanel buildSearchPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Search Everything");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JPanel top = new JPanel(new BorderLayout(8, 8));
        top.setOpaque(false);

        searchField = new JTextField();
        searchField.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        searchField.addActionListener(e -> doSearch());

        JButton btn = blackButton("Search");
        btn.addActionListener(e -> doSearch());

        top.add(searchField, BorderLayout.CENTER);
        top.add(btn, BorderLayout.EAST);

        searchModel = new DefaultListModel<>();
        searchList = new JList<>(searchModel);
        searchList.setFont(new Font("Consolas", Font.PLAIN, 13));
        searchList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) openSearchResult();
            }
        });

        JPanel north = new JPanel(new BorderLayout(8, 8));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);
        north.add(top, BorderLayout.SOUTH);

        page.add(north, BorderLayout.NORTH);
        page.add(new JScrollPane(searchList), BorderLayout.CENTER);
        return page;
    }

    private JPanel buildGreekSearchPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Greek Search");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JLabel help = new JLabel("Search imported Greek text, MorphGNT details, and verse references.");
        help.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        help.setForeground(new Color(90, 70, 60));

        JPanel input = new JPanel(new BorderLayout(8, 8));
        input.setOpaque(false);
        greekSearchField = new JTextField();
        greekSearchField.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        greekSearchField.addActionListener(e -> doGreekSearch());
        JButton searchBtn = blackButton("Search");
        searchBtn.addActionListener(e -> doGreekSearch());
        input.add(greekSearchField, BorderLayout.CENTER);
        input.add(searchBtn, BorderLayout.EAST);

        JPanel north = new JPanel(new BorderLayout(6, 6));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);
        north.add(help, BorderLayout.CENTER);
        north.add(input, BorderLayout.SOUTH);

        greekSearchModel = new DefaultListModel<>();
        greekSearchList = new JList<>(greekSearchModel);
        greekSearchList.setFont(new Font("Consolas", Font.PLAIN, 13));
        greekSearchList.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) previewGreekSearchSelection();
        });
        greekSearchList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) openGreekSearchSelection();
            }
        });

        greekSearchPreview = new JTextPane();
        greekSearchPreview.setEditable(false);
        greekSearchPreview.setFont(new Font("Consolas", Font.PLAIN, 13));
        greekSearchPreview.setText("Type a Greek word, reference, morphology tag, or details text, then click Search.");
        greekSearchPreview.setCaretPosition(0);

        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, new JScrollPane(greekSearchList), new JScrollPane(greekSearchPreview));
        split.setResizeWeight(0.38);
        split.setDividerSize(7);

        greekSearchStatus = new JLabel(" ");
        greekSearchStatus.setBorder(new EmptyBorder(4, 2, 0, 2));
        greekSearchStatus.setForeground(new Color(90, 70, 60));

        page.add(north, BorderLayout.NORTH);
        page.add(split, BorderLayout.CENTER);
        page.add(greekSearchStatus, BorderLayout.SOUTH);
        return page;
    }

    private JPanel buildRecentPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Notes Page");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, buildRecentNotesColumn(), buildChapterNotesColumn());
        split.setResizeWeight(0.50);
        split.setBorder(null);
        split.setContinuousLayout(true);
        split.setDividerSize(8);

        page.add(h, BorderLayout.NORTH);
        page.add(split, BorderLayout.CENTER);
        return page;
    }

    private JPanel buildRecentNotesColumn() {
        JPanel column = new JPanel(new BorderLayout(8, 8));
        column.setBackground(panelBg);
        column.setBorder(new CompoundBorder(new RoundedBorder(modernBorder, 14, new Insets(1, 1, 1, 1)), new EmptyBorder(12, 12, 12, 12)));

        JLabel h = new JLabel("Recent Notes");
        h.setFont(new Font("Segoe UI", Font.BOLD, 18));
        h.setForeground(darkRed);

        JPanel controls = new JPanel(new BorderLayout(8, 8));
        controls.setOpaque(false);

        recentSearchField = new JTextField();
        recentSearchField.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        recentSearchField.setToolTipText("Search recent annotation notes, highlights, questions, categories, Greek notes, and attachments.");
        recentSearchField.addActionListener(e -> refreshRecentNotes());
        recentSearchField.getDocument().addDocumentListener(new SimpleDocumentListener(this::refreshRecentNotes));

        recentFilterBox = new JComboBox<>(new String[]{"All", "Notes", "Categories", "Questions", "Greek", "Attachments"});
        recentFilterBox.setPreferredSize(new Dimension(145, 30));
        recentFilterBox.addActionListener(e -> refreshRecentNotes());

        JButton clear = blackButton("Clear");
        clear.addActionListener(e -> {
            recentSearchField.setText("");
            recentFilterBox.setSelectedItem("All");
            refreshRecentNotes();
        });

        JPanel right = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        right.setOpaque(false);
        right.add(new JLabel("Filter:"));
        right.add(recentFilterBox);
        right.add(clear);

        controls.add(recentSearchField, BorderLayout.CENTER);
        controls.add(right, BorderLayout.EAST);

        recentModel = new DefaultListModel<>();
        recentList = new JList<>(recentModel);
        recentList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        recentList.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        recentList.setFixedCellHeight(96);
        recentList.setCellRenderer(new RecentAnnotationCellRenderer());
        recentList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) openSelectedRecentAnnotation();
            }
            public void mousePressed(MouseEvent e) { maybeShowRecentNoteContextMenu(e); }
            public void mouseReleased(MouseEvent e) { maybeShowRecentNoteContextMenu(e); }
        });

        JPanel top = new JPanel(new BorderLayout(8, 8));
        top.setOpaque(false);
        top.add(h, BorderLayout.NORTH);
        top.add(controls, BorderLayout.SOUTH);
        column.add(top, BorderLayout.NORTH);
        column.add(new JScrollPane(recentList), BorderLayout.CENTER);
        return column;
    }

    private JPanel buildChapterNotesColumn() {
        JPanel column = new JPanel(new BorderLayout(8, 8));
        column.setBackground(panelBg);
        column.setBorder(new CompoundBorder(new RoundedBorder(modernBorder, 14, new Insets(1, 1, 1, 1)), new EmptyBorder(12, 12, 12, 12)));

        JLabel h = new JLabel("Chapter Notes");
        h.setFont(new Font("Segoe UI", Font.BOLD, 18));
        h.setForeground(darkRed);

        chapterNotesSearchField = new JTextField();
        chapterNotesSearchField.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        chapterNotesSearchField.setToolTipText("Search chapter notes by reference, source, text, or linked category/study/topic.");
        chapterNotesSearchField.getDocument().addDocumentListener(new SimpleDocumentListener(this::refreshChapterNotesList));

        JButton newCurrent = blackButton("Current Chapter ✎");
        newCurrent.setToolTipText("Edit notes for the currently selected book and chapter.");
        newCurrent.addActionListener(e -> openCurrentChapterNoteEditor());

        JPanel controls = new JPanel(new BorderLayout(8, 0));
        controls.setOpaque(false);
        controls.add(chapterNotesSearchField, BorderLayout.CENTER);
        controls.add(newCurrent, BorderLayout.EAST);

        chapterNotesModel = new DefaultListModel<>();
        chapterNotesList = new JList<>(chapterNotesModel);
        chapterNotesList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        chapterNotesList.setFixedCellHeight(118);
        chapterNotesList.setCellRenderer(new ChapterNoteCellRenderer());
        chapterNotesList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) openChapterNoteViewer(chapterNotesList.getSelectedValue());
            }
            public void mousePressed(MouseEvent e) { maybeShowChapterNoteContextMenu(e); }
            public void mouseReleased(MouseEvent e) { maybeShowChapterNoteContextMenu(e); }
        });

        JButton open = blackButton("Open");
        open.addActionListener(e -> openChapterNoteViewer(chapterNotesList.getSelectedValue()));
        JButton jump = blackButton("Jump To Chapter");
        jump.addActionListener(e -> jumpToChapterNote(chapterNotesList.getSelectedValue()));
        JButton delete = blackButton("Delete");
        delete.addActionListener(e -> deleteChapterNote(chapterNotesList.getSelectedValue()));
        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        actions.setOpaque(false);
        actions.add(open); actions.add(jump); actions.add(delete);

        JPanel top = new JPanel(new BorderLayout(8, 8));
        top.setOpaque(false);
        top.add(h, BorderLayout.NORTH);
        top.add(controls, BorderLayout.SOUTH);
        column.add(top, BorderLayout.NORTH);
        column.add(new JScrollPane(chapterNotesList), BorderLayout.CENTER);
        column.add(actions, BorderLayout.SOUTH);
        return column;
    }

    private JPanel buildCategoriesPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Categories");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JButton add = blackButton("Create Category");
        JButton view = blackButton("View Selected");
        JButton color = blackButton("Change Highlight Color");

        add.addActionListener(e -> createCategory(null));
        view.addActionListener(e -> viewSelectedCategory());
        color.addActionListener(e -> changeSelectedCategoryColor());

        categorySearchField = new JTextField(22);
        categorySearchField.setToolTipText("Filter categories...");
        categorySearchField.getDocument().addDocumentListener(new SimpleDocumentListener(this::refreshCategories));

        JPanel leftControls = new JPanel();
        leftControls.setOpaque(false);
        leftControls.setLayout(new BoxLayout(leftControls, BoxLayout.Y_AXIS));

        JPanel leftButtons = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        leftButtons.setOpaque(false);
        leftButtons.add(add);
        leftButtons.add(view);
        leftButtons.add(color);

        JPanel filterRow = new JPanel(new BorderLayout(6, 0));
        filterRow.setOpaque(false);
        filterRow.setBorder(new EmptyBorder(8, 0, 8, 0));
        filterRow.add(new JLabel("Filter:"), BorderLayout.WEST);
        filterRow.add(categorySearchField, BorderLayout.CENTER);

        leftControls.add(leftButtons);
        leftControls.add(filterRow);

        categoryModel = new DefaultListModel<>();
        categoryList = new JList<>(categoryModel);
        categoryList.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        categoryList.setCellRenderer(new CategoryCellRenderer());
        categoryList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        categoryList.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) renderSelectedCategoryResults();
        });
        categoryList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (e.getClickCount() == 2) viewSelectedCategory();
            }
        });

        JScrollPane categoryScroll = new JScrollPane(categoryList);
        categoryScroll.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        categoryScroll.setBorder(new CompoundBorder(new LineBorder(modernBorder), new EmptyBorder(0, 0, 0, 0)));

        JPanel left = new JPanel(new BorderLayout(8, 8));
        left.setOpaque(false);
        left.add(leftControls, BorderLayout.NORTH);
        left.add(categoryScroll, BorderLayout.CENTER);

        selectedCategoryTitleLabel = new JLabel("Select a category");
        selectedCategoryTitleLabel.setFont(new Font("Segoe UI", Font.BOLD, 20));
        selectedCategoryTitleLabel.setForeground(darkRed);

        categoryResultSearchField = new JTextField(24);
        categoryResultSearchField.setToolTipText("Search within the selected category...");
        categoryResultSearchField.getDocument().addDocumentListener(new SimpleDocumentListener(this::renderSelectedCategoryResults));

        JButton expandAll = blackButton("Expand All");
        JButton collapseAll = blackButton("Collapse All");
        expandAll.addActionListener(e -> expandAllCategoryResults());
        collapseAll.addActionListener(e -> collapseAllCategoryResults());

        JPanel resultTools = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        resultTools.setOpaque(false);
        resultTools.add(new JLabel("Search results:"));
        resultTools.add(categoryResultSearchField);
        resultTools.add(expandAll);
        resultTools.add(collapseAll);

        JPanel rightHeader = new JPanel(new BorderLayout(8, 8));
        rightHeader.setOpaque(false);
        rightHeader.add(selectedCategoryTitleLabel, BorderLayout.NORTH);
        rightHeader.add(resultTools, BorderLayout.SOUTH);

        categoryResultsPanel = new JPanel();
        categoryResultsPanel.setBackground(modernSurface);
        categoryResultsPanel.setLayout(new BoxLayout(categoryResultsPanel, BoxLayout.Y_AXIS));
        categoryResultsScroll = new JScrollPane(categoryResultsPanel);
        categoryResultsScroll.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        categoryResultsScroll.getVerticalScrollBar().setUnitIncrement(16);
        categoryResultsScroll.setBorder(new CompoundBorder(new LineBorder(modernBorder), new EmptyBorder(0, 0, 0, 0)));

        JPanel right = new JPanel(new BorderLayout(8, 8));
        right.setOpaque(false);
        right.add(rightHeader, BorderLayout.NORTH);
        right.add(categoryResultsScroll, BorderLayout.CENTER);

        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, right);
        split.setResizeWeight(0.25);
        split.setDividerLocation(260);
        split.setContinuousLayout(true);
        split.setBorder(null);

        JPanel north = new JPanel(new BorderLayout(8, 8));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);

        page.add(north, BorderLayout.NORTH);
        page.add(split, BorderLayout.CENTER);
        return page;
    }

    private JPanel buildQuestionsPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Questions");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        top.setOpaque(false);

        JButton toggle = blackButton("Toggle Answered / Unanswered");
        JButton add = blackButton("Add Question To Current Selection");
        JButton addTopic = blackButton("Add Question to Topic Page");
        toggle.addActionListener(e -> toggleSelectedQuestion());
        add.addActionListener(e -> addQuestionForSelection());
        addTopic.addActionListener(e -> addSelectedQuestionToTopicPage());

        questionSearchField = new JTextField(22);
        questionSearchField.setToolTipText("Filter questions...");
        questionSearchField.getDocument().addDocumentListener(new SimpleDocumentListener(this::refreshQuestions));
        top.add(toggle);
        top.add(add);
        top.add(addTopic);
        top.add(new JLabel("Filter:"));
        top.add(questionSearchField);

        questionModel = new DefaultListModel<>();
        questionList = new JList<>(questionModel);
        questionList.setFont(new Font("Segoe UI", Font.PLAIN, 15));

        JPanel north = new JPanel(new BorderLayout(8, 8));
        north.setOpaque(false);
        north.add(h, BorderLayout.NORTH);
        north.add(top, BorderLayout.SOUTH);

        discussionQuestionsPanel = new JPanel();
        discussionQuestionsPanel.setLayout(new BoxLayout(discussionQuestionsPanel, BoxLayout.Y_AXIS));
        discussionQuestionsPanel.setBackground(modernSurface);
        personalQuestionsPanel = new JPanel();
        personalQuestionsPanel.setLayout(new BoxLayout(personalQuestionsPanel, BoxLayout.Y_AXIS));
        personalQuestionsPanel.setBackground(modernSurface);

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
                labeledQuestionSection("Discussion Questions", discussionQuestionsPanel),
                labeledQuestionSection("Personal Questions", personalQuestionsPanel));
        split.setResizeWeight(0.5);
        split.setBorder(null);

        page.add(north, BorderLayout.NORTH);
        page.add(split, BorderLayout.CENTER);
        return page;
    }



    private JComponent labeledQuestionSection(String title, JPanel body) {
        JPanel panel = new JPanel(new BorderLayout(8, 8));
        panel.setBackground(panelBg);
        panel.setBorder(new EmptyBorder(6, 0, 6, 0));
        JLabel label = new JLabel(title);
        label.setFont(new Font("Segoe UI", Font.BOLD, 18));
        label.setForeground(darkRed);
        panel.add(label, BorderLayout.NORTH);
        JScrollPane scroll = new JScrollPane(body);
        scroll.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        scroll.getVerticalScrollBar().setUnitIncrement(16);
        panel.add(scroll, BorderLayout.CENTER);
        return panel;
    }

    private JPanel buildTopicPagesPage() {
        JPanel page = new JPanel(new BorderLayout(10, 10));
        page.setBorder(new EmptyBorder(16, 16, 16, 16));
        page.setBackground(panelBg);

        JLabel h = new JLabel("Topic Pages");
        h.setFont(new Font("Segoe UI", Font.BOLD, 26));
        h.setForeground(darkRed);

        topicPageModel = new DefaultListModel<>();
        topicPageList = new JList<>(topicPageModel);
        topicPageList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        topicPageList.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        topicPageList.addListSelectionListener(e -> {
            if (!e.getValueIsAdjusting()) refreshSelectedTopicDetails();
        });

        JButton create = blackButton("Create Topic");
        JButton rename = blackButton("Rename Topic");
        JButton delete = blackButton("Delete Topic");
        create.addActionListener(e -> createTopicPageDialog());
        rename.addActionListener(e -> renameSelectedTopicPage());
        delete.addActionListener(e -> deleteSelectedTopicPage());

        JPanel leftButtons = new JPanel(new GridLayout(0, 1, 6, 6));
        leftButtons.setOpaque(false);
        leftButtons.add(create);
        leftButtons.add(rename);
        leftButtons.add(delete);

        JPanel left = new JPanel(new BorderLayout(8, 8));
        left.setOpaque(false);
        topicPageSearchField = new JTextField();
        topicPageSearchField.setToolTipText("Filter topic pages...");
        topicPageSearchField.getDocument().addDocumentListener(new SimpleDocumentListener(this::refreshTopicPages));
        left.add(topicPageSearchField, BorderLayout.NORTH);
        left.add(new JScrollPane(topicPageList), BorderLayout.CENTER);
        left.add(leftButtons, BorderLayout.SOUTH);
        left.setPreferredSize(new Dimension(280, 10));

        topicTitleLabel = new JLabel("Select or create a topic");
        topicTitleLabel.setFont(new Font("Segoe UI", Font.BOLD, 20));
        topicTitleLabel.setForeground(darkRed);

        topicSummaryArea = new JTextArea(7, 40);
        topicSummaryArea.setLineWrap(true);
        topicSummaryArea.setWrapStyleWord(true);
        topicSummaryArea.setFont(new Font("Segoe UI", Font.PLAIN, 14));

        JButton saveSummary = blackButton("Save Summary");
        saveSummary.addActionListener(e -> saveSelectedTopicSummary());

        topicLinkModel = new DefaultListModel<>();
        topicLinkList = new JList<>(topicLinkModel);
        topicLinkList.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        topicLinkList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) openSelectedTopicLink();
            }
        });

        JButton addVerse = blackButton("Add Current Verse");
        JButton addSelection = blackButton("Add Current Selection");
        JButton addExisting = blackButton("Add Existing Note/Question");
        JButton removeLink = blackButton("Remove Selected Link");
        JButton openLink = blackButton("Open Selected Link");
        addVerse.addActionListener(e -> addCurrentVerseToSelectedTopic());
        addSelection.addActionListener(e -> addCurrentSelectionToSelectedTopic());
        addExisting.addActionListener(e -> addExistingNoteOrQuestionToSelectedTopic());
        removeLink.addActionListener(e -> removeSelectedTopicLink());
        openLink.addActionListener(e -> openSelectedTopicLink());

        JPanel linkButtons = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 4));
        linkButtons.setOpaque(false);
        linkButtons.add(addVerse);
        linkButtons.add(addSelection);
        linkButtons.add(addExisting);
        linkButtons.add(removeLink);
        linkButtons.add(openLink);

        JPanel summaryPanel = new JPanel(new BorderLayout(6, 6));
        summaryPanel.setOpaque(false);
        summaryPanel.add(topicTitleLabel, BorderLayout.NORTH);
        summaryPanel.add(new JScrollPane(topicSummaryArea), BorderLayout.CENTER);
        summaryPanel.add(saveSummary, BorderLayout.SOUTH);

        JPanel right = new JPanel(new BorderLayout(8, 8));
        right.setOpaque(false);
        right.add(summaryPanel, BorderLayout.NORTH);
        right.add(new JScrollPane(topicLinkList), BorderLayout.CENTER);
        right.add(linkButtons, BorderLayout.SOUTH);

        JSplitPane split = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, left, right);
        split.setResizeWeight(0.25);
        split.setDividerSize(7);

        page.add(h, BorderLayout.NORTH);
        page.add(split, BorderLayout.CENTER);
        return page;
    }

    private enum ButtonType { PRIMARY, SECONDARY, DANGER, UTILITY, NAV, ACTIVE_NAV }

    private JButton navButton(String s) {
        JButton b = new ModernNavButton(s);
        b.putClientProperty("buttonType", ButtonType.NAV);
        b.putClientProperty("navButton", Boolean.TRUE);
        styleModernButton(b, ButtonType.NAV);
        return b;
    }

    private JButton blackButton(String s) {
        JButton b = styledButton(s, new Color(255, 248, 240), Color.BLACK);
        String lower = s.toLowerCase(Locale.ROOT);
        ButtonType type = (lower.contains("delete") || lower.contains("clear") || lower.contains("remove"))
                ? ButtonType.DANGER : ButtonType.SECONDARY;
        b.putClientProperty("buttonType", type);
        styleModernButton(b, type);
        return b;
    }

    private JButton styledButton(String s, Color bg, Color fg) {
        JButton b = new JButton(s);
        b.setFont(new Font("Segoe UI", Font.BOLD, 12));
        b.setFocusPainted(false);
        b.setBackground(bg);
        b.setForeground(fg);
        b.setBorder(new CompoundBorder(new LineBorder(new Color(120, 60, 60)), new EmptyBorder(6, 10, 6, 10)));
        b.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        b.setMinimumSize(new Dimension(86, 32));
        return b;
    }

    private JPanel createNavGroup(Component... components) {
        JPanel group = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        group.setOpaque(false);
        group.setBorder(new CompoundBorder(
                new LineBorder(new Color(255, 255, 255, 45), 1, true),
                new EmptyBorder(6, 7, 6, 7)
        ));
        for (Component c : components) group.add(c);
        return group;
    }

    /**
     * Modern theme helpers. These methods intentionally style by component type so
     * existing feature code can keep building Swing controls normally while the
     * visual language stays consistent across pages, sidebars, dialogs, and lists.
     */
    private boolean isModernViewEnabled() {
        return data == null || data.modernViewEnabled == null || data.modernViewEnabled;
    }

    private void toggleModernView() {
        data.modernViewEnabled = !isModernViewEnabled();
        saveData();
        applyModernTheme(this);
        updateModernToggleText();
        updateActiveNavButton();
        revalidate();
        repaint();
        log((isModernViewEnabled() ? "Modern" : "Classic") + " view enabled");
    }

    private void updateModernToggleText() {
        if (modernViewToggleButton != null) {
            modernViewToggleButton.setText(isModernViewEnabled() ? "Modern View: On" : "Classic View");
            modernViewToggleButton.putClientProperty("buttonType", isModernViewEnabled() ? ButtonType.ACTIVE_NAV : ButtonType.NAV);
        }
    }

    private void styleModernButton(JButton b, ButtonType type) {
        if (b == null) return;
        if (type == null) type = ButtonType.SECONDARY;
        b.putClientProperty("buttonType", type);
        b.setFocusPainted(false);
        boolean nav = type == ButtonType.NAV || type == ButtonType.ACTIVE_NAV || Boolean.TRUE.equals(b.getClientProperty("navButton"));
        b.setOpaque(!nav);
        b.setContentAreaFilled(!nav);
        b.setBorderPainted(!nav);
        b.setRolloverEnabled(true);
        b.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        b.setFont(type == ButtonType.UTILITY ? modernBaseFont.deriveFont(Font.BOLD, 12f) : modernBoldFont.deriveFont(13f));
        b.setMargin(new Insets(0, 0, 0, 0));
        int height = nav ? 34 : 36;
        b.setMinimumSize(new Dimension(type == ButtonType.UTILITY ? 66 : 92, height));
        if (nav) {
            b.setBorder(new EmptyBorder(8, 14, 8, 14));
        } else {
            b.setBorder(new RoundedBorder(type == ButtonType.PRIMARY || type == ButtonType.DANGER ? modernDarkRed : modernBorder, 13,
                    type == ButtonType.UTILITY ? new Insets(5, 9, 5, 9) : new Insets(8, 13, 8, 13)));
        }
        if (!Boolean.TRUE.equals(b.getClientProperty("modernStateListenerInstalled"))) {
            b.getModel().addChangeListener(e -> applyButtonState(b));
            b.putClientProperty("modernStateListenerInstalled", Boolean.TRUE);
        }
        applyButtonState(b);
    }

    private void applyButtonState(JButton b) {
        ButtonType type = (ButtonType) b.getClientProperty("buttonType");
        if (type == null) type = ButtonType.SECONDARY;
        boolean modern = isModernViewEnabled();
        boolean nav = type == ButtonType.NAV || type == ButtonType.ACTIVE_NAV || Boolean.TRUE.equals(b.getClientProperty("navButton"));
        if (!b.isEnabled()) {
            b.setBackground(modern ? modernDisabled : new Color(225, 220, 215));
            b.setForeground(modern ? modernMutedText : Color.GRAY);
            if (nav) {
                b.putClientProperty("navBorderColor", modern ? new Color(modernDarkRed.getRed(), modernDarkRed.getGreen(), modernDarkRed.getBlue(), 55) : new Color(120, 60, 60, 90));
                b.repaint();
            }
            return;
        }
        ButtonModel model = b.getModel();
        boolean pressed = model.isPressed() && model.isArmed();
        boolean rollover = model.isRollover();
        if (!modern) {
            if (nav) {
                Color bg = pressed ? new Color(255, 226, 206, 210) : (rollover ? new Color(255, 238, 224, 185) : new Color(255, 248, 240, 150));
                b.setBackground(bg);
                b.setForeground(Color.BLACK);
                b.putClientProperty("navBorderColor", new Color(120, 60, 60, rollover || pressed ? 175 : 130));
                b.setBorder(new EmptyBorder(8, 14, 8, 14));
                b.repaint();
            } else {
                b.setBackground(rollover ? new Color(255, 238, 224) : new Color(255, 248, 240));
                b.setForeground(Color.BLACK);
                b.setBorder(new CompoundBorder(new LineBorder(new Color(120, 60, 60)), new EmptyBorder(6, 10, 6, 10)));
            }
            return;
        }
        Color bg;
        Color fg;
        switch (type) {
            case PRIMARY:
                bg = pressed ? modernDarkRed : (rollover ? new Color(130, 36, 40) : modernPrimaryRed); fg = Color.WHITE; break;
            case DANGER:
                bg = pressed ? new Color(120, 36, 40) : (rollover ? new Color(174, 68, 72) : modernDanger); fg = Color.WHITE; break;
            case NAV:
                bg = pressed ? new Color(255, 238, 224, 118) : (rollover ? new Color(255, 248, 240, 92) : new Color(255, 248, 240, 48)); fg = new Color(255, 248, 240); break;
            case ACTIVE_NAV:
                bg = pressed ? new Color(236, 211, 196, 225) : (rollover ? new Color(244, 221, 207, 232) : new Color(239, 215, 201, 218)); fg = modernDarkRed; break;
            case UTILITY:
                bg = pressed ? new Color(232, 222, 211) : (rollover ? new Color(255, 249, 241) : modernSurface); fg = modernText; break;
            case SECONDARY:
            default:
                bg = pressed ? new Color(232, 222, 211) : (rollover ? Color.WHITE : modernSurface); fg = modernText;
        }
        b.setBackground(bg);
        b.setForeground(fg);
        if (nav) {
            Color border = type == ButtonType.ACTIVE_NAV
                    ? new Color(modernDarkRed.getRed(), modernDarkRed.getGreen(), modernDarkRed.getBlue(), pressed ? 170 : (rollover ? 150 : 130))
                    : new Color(255, 248, 240, pressed ? 112 : (rollover ? 92 : 62));
            b.putClientProperty("navBorderColor", border);
            b.setBorder(new EmptyBorder(8, 14, 8, 14));
            b.repaint();
        } else {
            b.setBorder(new RoundedBorder(type == ButtonType.PRIMARY || type == ButtonType.DANGER ? bg.darker() : modernBorder, 13,
                    type == ButtonType.UTILITY ? new Insets(5, 9, 5, 9) : new Insets(8, 13, 8, 13)));
        }
    }

    private void styleModernPanel(JPanel p) {
        if (p == null || !p.isOpaque()) return;
        if (Boolean.TRUE.equals(p.getClientProperty("modernHeader"))) {
            p.setBackground(isModernViewEnabled() ? modernDarkRed : darkRed);
        } else {
            p.setBackground(isModernViewEnabled() ? modernBackground : panelBg);
        }
    }

    private void styleModernCard(JComponent c) {
        if (c == null) return;
        c.putClientProperty("modernCard", Boolean.TRUE);
        c.setOpaque(true);
        c.setBackground(isModernViewEnabled() ? modernSurface : cream);
        c.setBorder(isModernViewEnabled()
                ? new CompoundBorder(new RoundedBorder(modernBorder, 16, new Insets(1, 1, 1, 1)), new EmptyBorder(10, 10, 10, 10))
                : new CompoundBorder(new LineBorder(new Color(180, 145, 135)), new EmptyBorder(8, 8, 8, 8)));
    }

    private void styleCompactSidebarCard(JComponent c) {
        if (c == null) return;
        styleModernCard(c);
        c.putClientProperty("compactSidebarCard", Boolean.TRUE);
        applyCompactSidebarCardBorder(c);
    }

    private void applyCompactSidebarCardBorder(JComponent c) {
        if (c == null || !Boolean.TRUE.equals(c.getClientProperty("compactSidebarCard"))) return;
        c.setBorder(isModernViewEnabled()
                ? new CompoundBorder(new RoundedBorder(modernBorder, 14, new Insets(1, 1, 1, 1)), new EmptyBorder(5, 5, 5, 5))
                : new CompoundBorder(new LineBorder(new Color(180, 145, 135)), new EmptyBorder(4, 4, 4, 4)));
    }

    private void styleModernInput(JComponent c) {
        if (c == null) return;
        c.setFont(modernBaseFont);
        c.setBackground(isModernViewEnabled() ? Color.WHITE : cream);
        c.setForeground(isModernViewEnabled() ? modernText : Color.BLACK);
        c.setBorder(new CompoundBorder(new RoundedBorder(isModernViewEnabled() ? modernBorder : new Color(180, 145, 135), 10, new Insets(1, 1, 1, 1)), new EmptyBorder(6, 9, 6, 9)));
        c.setMinimumSize(new Dimension(90, 34));
    }

    private void styleModernList(JList<?> list) {
        if (list == null) return;
        list.setFont(modernBaseFont);
        list.setBackground(isModernViewEnabled() ? modernSurface : cream);
        list.setForeground(isModernViewEnabled() ? modernText : Color.BLACK);
        list.setSelectionBackground(isModernViewEnabled() ? modernSelection : new Color(210, 225, 245));
        list.setSelectionForeground(isModernViewEnabled() ? modernText : Color.BLACK);
        list.setFixedCellHeight(list.getFixedCellHeight() > 0 ? list.getFixedCellHeight() : 28);
        list.setBorder(new EmptyBorder(4, 4, 4, 4));
    }

    private void styleModernScrollPane(JScrollPane sp) {
        if (sp == null) return;
        sp.setBorder(isModernViewEnabled() ? new RoundedBorder(modernBorder, 14, new Insets(1, 1, 1, 1)) : null);
        sp.getViewport().setBackground(isModernViewEnabled() ? modernSurface : cream);
        sp.getVerticalScrollBar().setUnitIncrement(16);
        sp.getHorizontalScrollBar().setUnitIncrement(16);
    }

    private void applyModernTheme(Component root) {
        if (root == null) return;
        boolean modern = isModernViewEnabled();
        UIManager.put("OptionPane.background", modern ? modernBackground : panelBg);
        UIManager.put("Panel.background", modern ? modernBackground : panelBg);
        UIManager.put("Button.rollover", Boolean.TRUE);

        if (root instanceof JFrame) ((JFrame) root).getContentPane().setBackground(modern ? modernBackground : panelBg);
        if (root instanceof JDialog) ((JDialog) root).getContentPane().setBackground(modern ? modernBackground : panelBg);
        if (root instanceof JPanel) styleModernPanel((JPanel) root);
        if (root instanceof JComponent && Boolean.TRUE.equals(((JComponent) root).getClientProperty("modernCard"))) {
            JComponent component = (JComponent) root;
            styleModernCard(component);
            applyCompactSidebarCardBorder(component);
        }
        if (root instanceof JButton) {
            ButtonType type = (ButtonType) ((JButton) root).getClientProperty("buttonType");
            styleModernButton((JButton) root, type == null ? ButtonType.SECONDARY : type);
        }
        if (root instanceof JTextField || root instanceof JTextArea || root instanceof JTextPane || root instanceof JComboBox) styleModernInput((JComponent) root);
        if (root instanceof JList) styleModernList((JList<?>) root);
        if (root instanceof JScrollPane) styleModernScrollPane((JScrollPane) root);
        if (root instanceof JSplitPane) {
            JSplitPane split = (JSplitPane) root;
            split.setDividerSize(modern ? 8 : 7);
            split.setBorder(null);
            split.setBackground(modern ? modernBackground : panelBg);
        }
        if (root instanceof JTree) {
            JTree t = (JTree) root;
            t.setFont(modernBaseFont);
            t.setBackground(modern ? modernSurface : cream);
            t.setForeground(modern ? modernText : Color.BLACK);
            t.setRowHeight(28);
        }
        if (root instanceof Container) {
            for (Component child : ((Container) root).getComponents()) applyModernTheme(child);
        }
        if (root == this) {
            updateModernToggleText();
            updateActiveNavButton();
            updateRightSidebarToggleButtons();
        }
    }

    private static class WidthTrackingPanel extends JPanel implements Scrollable {
        public Dimension getPreferredScrollableViewportSize() { return getPreferredSize(); }
        public int getScrollableUnitIncrement(Rectangle visibleRect, int orientation, int direction) { return 16; }
        public int getScrollableBlockIncrement(Rectangle visibleRect, int orientation, int direction) { return Math.max(16, visibleRect.height - 16); }
        public boolean getScrollableTracksViewportWidth() { return true; }
        public boolean getScrollableTracksViewportHeight() { return false; }
    }

    private class LibraryTreeCellRenderer extends DefaultTreeCellRenderer {
        private static final long serialVersionUID = 1L;

        @Override
        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected, boolean expanded,
                                                      boolean leaf, int row, boolean hasFocus) {
            Component component = super.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);
            Object userObject = value instanceof DefaultMutableTreeNode ? ((DefaultMutableTreeNode) value).getUserObject() : value;
            String displayName = getLibraryDisplayName(userObject);
            setText(displayName);
            setToolTipText(displayName);
            return component;
        }
    }

    private static class LibraryRow {
        final TreePath path;
        final Object value;

        LibraryRow(TreePath path, Object value) {
            this.path = path;
            this.value = value;
        }
    }

    private static class BookTreeItem {
        final String bookKey;
        final String displayName;

        BookTreeItem(String bookKey, String displayName) {
            this.bookKey = bookKey == null ? "" : bookKey;
            this.displayName = displayName == null || displayName.trim().isEmpty() ? this.bookKey : displayName;
        }

        public String toString() { return displayName; }

        public boolean equals(Object other) {
            if (this == other) return true;
            if (other instanceof BookTreeItem) return bookKey.equals(((BookTreeItem) other).bookKey);
            if (other instanceof String) return bookKey.equals(other);
            return false;
        }

        public int hashCode() { return bookKey.hashCode(); }
    }

    private static class LibraryTreeItem {
        final String title;

        LibraryTreeItem(String title) {
            this.title = title == null ? "" : title;
        }

        public String toString() { return title; }

        public boolean equals(Object other) {
            if (this == other) return true;
            if (other instanceof LibraryTreeItem) return title.equals(((LibraryTreeItem) other).title);
            if (other instanceof String) return title.equals(other);
            return false;
        }

        public int hashCode() { return title.hashCode(); }
    }

    private static class TreeSourceItem {
        final String displayName;
        final String sourceKey;

        TreeSourceItem(String displayName, String sourceKey) {
            this.displayName = displayName == null ? "" : displayName;
            this.sourceKey = sourceKey == null ? "" : sourceKey;
        }

        public String toString() { return displayName; }
    }

    private static class ModernNavButton extends JButton {
        private static final long serialVersionUID = 1L;
        private static final int ARC = 22;

        ModernNavButton(String text) {
            super(text);
            setOpaque(false);
            setContentAreaFilled(false);
            setBorderPainted(false);
        }

        @Override
        protected void paintComponent(Graphics g) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            int inset = 1;
            int width = getWidth() - 1 - (inset * 2);
            int height = getHeight() - 1 - (inset * 2);
            if (width > 0 && height > 0) {
                g2.setColor(getBackground());
                g2.fillRoundRect(inset, inset, width, height, ARC, ARC);
                Object borderColor = getClientProperty("navBorderColor");
                g2.setColor(borderColor instanceof Color ? (Color) borderColor : new Color(255, 248, 240, 70));
                g2.drawRoundRect(inset, inset, width, height, ARC, ARC);
            }
            g2.dispose();
            super.paintComponent(g);
        }
    }

    private static class RoundedBorder extends AbstractBorder {
        private final Color color;
        private final int radius;
        private final Insets insets;

        RoundedBorder(Color color, int radius, Insets insets) {
            this.color = color;
            this.radius = radius;
            this.insets = insets;
        }

        public Insets getBorderInsets(Component c) { return insets; }
        public Insets getBorderInsets(Component c, Insets target) {
            target.top = insets.top; target.left = insets.left; target.bottom = insets.bottom; target.right = insets.right;
            return target;
        }
        public void paintBorder(Component c, Graphics g, int x, int y, int width, int height) {
            Graphics2D g2 = (Graphics2D) g.create();
            g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2.setColor(color);
            g2.drawRoundRect(x, y, width - 1, height - 1, radius, radius);
            g2.dispose();
        }
    }

    private JLabel labelWhite(String s) {
        JLabel l = new JLabel(s);
        l.setForeground(Color.WHITE);
        l.setFont(new Font("Segoe UI", Font.BOLD, 12));
        return l;
    }

    private JTextArea readonlyArea() {
        JTextArea a = new JTextArea();
        a.setEditable(false);
        a.setLineWrap(true);
        a.setWrapStyleWord(true);
        a.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        a.setBackground(cream);
        a.setBorder(new EmptyBorder(10, 10, 10, 10));
        return a;
    }

    private void showCard(String name) {
        activeCardName = name;
        cards.show(cardPanel, name);
        updateActiveNavButton();
        if (statusLabel != null) statusLabel.setText(" " + displayCardName(name));
    }

    private String displayCardName(String name) {
        if ("study".equals(name)) return "Study";
        if ("greekSearch".equals(name)) return "Greek Search";
        if ("memory".equals(name)) return "Memory Verses";
        if ("studyProjects".equals(name)) return "Study Projects";
        if ("recent".equals(name)) return "Notes Page";
        if ("topicPages".equals(name)) return "Topic Pages";
        if (name == null || name.isEmpty()) return "Ready";
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    private void updateActiveNavButton() {
        for (Map.Entry<String, JButton> entry : navButtonsByCard.entrySet()) {
            JButton b = entry.getValue();
            b.putClientProperty("buttonType", entry.getKey().equals(activeCardName) ? ButtonType.ACTIVE_NAV : ButtonType.NAV);
            applyButtonState(b);
        }
        if (modernViewToggleButton != null) {
            modernViewToggleButton.putClientProperty("buttonType", isModernViewEnabled() ? ButtonType.ACTIVE_NAV : ButtonType.NAV);
            applyButtonState(modernViewToggleButton);
        }
    }

    private void log(String s) {
        Runnable r = () -> {
            if (importLog != null) {
                importLog.append(new SimpleDateFormat("HH:mm:ss").format(new Date()) + " - " + s + "\n");
                importLog.setCaretPosition(importLog.getDocument().getLength());
            }
            if (statusLabel != null) statusLabel.setText(" " + s);
        };
        if (SwingUtilities.isEventDispatchThread()) r.run();
        else SwingUtilities.invokeLater(r);
    }

    private void refreshEverything() {
        refreshingUi = true;
        try {
            refreshProfiles();
            refreshLibraryTree();
            refreshBookCombo();
            refreshCategories();
            refreshQuestions();
            refreshTopicPages();
            refreshMemoryVerses();
            refreshRecentNotes();
            refreshStudyProjects();
            refreshPinnedItems();
            refreshRecentlyOpened();
            updateHistoryButtons();
            updateHeader();
        } finally {
            refreshingUi = false;
        }

        if (!selectedBook.isEmpty() && data.bible.containsKey(selectedBook)) showSelectedChapter(false);
        else if (!data.bible.isEmpty()) {
            selectedBook = orderedBooks().get(0);
            selectedChapter = data.getChapters(selectedBook).iterator().next();
            showSelectedChapter(false);
        } else {
            showBlankReader();
        }
    }

    private void updateHeader() {
        if (profileLabel == null || currentProfile == null) return;
        profileLabel.setText("Profile: " + currentProfile.name +
                " | Bible verses: " + data.totalVerseCount() +
                " | Greek entries: " + data.greek.size() +
                " | Library works: " + data.libraryDocs.size() +
                " | Text notes: " + currentProfile.annotations.size() +
                " | Memory verses: " + currentProfile.memoryVerses.size() +
                " | Unfinished questions: " + countUnanswered());
    }

    private void refreshProfiles() {
        if (profileBox == null) return;
        profileBox.removeAllItems();
        for (String n : data.profiles.keySet()) profileBox.addItem(n);
        if (currentProfile != null) profileBox.setSelectedItem(currentProfile.name);
    }

    private void switchProfile() {
        if (refreshingUi) return;
        Object o = profileBox.getSelectedItem();
        if (o == null) return;
        Profile p = data.profiles.get(o.toString());
        if (p != null && p != currentProfile) {
            currentProfile = p;
            repairProfile(currentProfile);
            refreshEverything();
        }
    }

    private void createProfile() {
        String n = JOptionPane.showInputDialog(this, "Profile name:");
        if (n == null) return;
        n = n.trim();
        if (n.isEmpty()) return;
        if (data.profiles.containsKey(n)) {
            JOptionPane.showMessageDialog(this, "That profile already exists.");
            return;
        }
        Profile p = new Profile(n);
        data.profiles.put(n, p);
        currentProfile = p;
        saveData();
        refreshEverything();
    }

    private void refreshLibraryTree() {
        if (rootNode == null) return;
        rootNode.removeAllChildren();

        DefaultMutableTreeNode bible = new DefaultMutableTreeNode("Bible");
        for (String book : orderedBooks()) {
            DefaultMutableTreeNode bn = new DefaultMutableTreeNode(new BookTreeItem(book, displayBibleBookName(book)));
            for (Integer ch : data.getChapters(book)) {
                String legacyKey = "BIBLE:" + book + " " + ch;
                String sourceKey = "BIBLE:" + book + ":" + ch;
                int visits = currentProfile.visitCounts.getOrDefault(legacyKey, currentProfile.visitCounts.getOrDefault(sourceKey, 0));
                String displayName = "Chapter " + ch + (visits > 0 ? " (" + visits + " visits)" : "");
                bn.add(new DefaultMutableTreeNode(new TreeSourceItem(displayName, sourceKey)));
            }
            bible.add(bn);
        }

        DefaultMutableTreeNode philosophy = new DefaultMutableTreeNode("Philosophy / Other");
        for (LibraryDoc d : data.libraryDocs) philosophy.add(new DefaultMutableTreeNode(new LibraryTreeItem(d.title)));

        rootNode.add(bible);
        rootNode.add(philosophy);
        treeModel.reload();
        libraryTree.expandRow(0);
        if (bibleTreeExpanded) expandBibleTreeRows();
        libraryTree.repaint();
    }

    private void collapseBibleTree() {
        bibleTreeExpanded = false;
        if (libraryTree == null) return;
        TreePath biblePath = findTopLevelTreePath("Bible");
        if (biblePath == null) return;
        for (int row = libraryTree.getRowCount() - 1; row >= 0; row--) {
            TreePath path = libraryTree.getPathForRow(row);
            if (path != null && biblePath.isDescendant(path) && path.getPathCount() > biblePath.getPathCount()) {
                libraryTree.collapsePath(path);
            }
        }
        libraryTree.collapsePath(biblePath);
        libraryTree.expandRow(0);
    }

    private void expandBibleTree() {
        bibleTreeExpanded = true;
        expandBibleTreeRows();
    }

    private void expandBibleTreeRows() {
        if (libraryTree == null) return;
        TreePath biblePath = findTopLevelTreePath("Bible");
        if (biblePath == null) return;
        libraryTree.expandPath(biblePath);
        for (int row = 0; row < libraryTree.getRowCount(); row++) {
            TreePath path = libraryTree.getPathForRow(row);
            if (path != null && biblePath.isDescendant(path)) libraryTree.expandRow(row);
        }
    }

    private TreePath findTopLevelTreePath(String label) {
        if (rootNode == null) return null;
        Enumeration<?> children = rootNode.children();
        while (children.hasMoreElements()) {
            DefaultMutableTreeNode child = (DefaultMutableTreeNode) children.nextElement();
            if (label.equals(String.valueOf(child.getUserObject()))) return new TreePath(child.getPath());
        }
        return null;
    }

    private void maybeOpenLibraryRow(MouseEvent e) {
        if (refreshingUi || libraryTree == null || e == null || !SwingUtilities.isLeftMouseButton(e)) return;
        TreePath path = libraryTree.getPathForLocation(e.getX(), e.getY());
        if (path == null) return;
        openLibraryNode(userObjectFromTreePath(path), true);
    }

    private boolean openLibraryNode(Object nodeValue, boolean honorBookmark) {
        String scopeKey = getBookmarkScopeKeyForLibraryNode(nodeValue);
        if (honorBookmark) {
            StudyBookmark bookmark = findBookmarkForScopeKey(scopeKey);
            if (bookmark != null) {
                openBookmark(bookmark);
                return true;
            }
        }

        Object value = nodeValue instanceof DefaultMutableTreeNode ? ((DefaultMutableTreeNode) nodeValue).getUserObject() : nodeValue;
        if (value instanceof TreeSourceItem) {
            String sourceKey = ((TreeSourceItem) value).sourceKey;
            if (safe(sourceKey).isEmpty()) return false;
            openSourceKey(sourceKey);
            showCard("study");
            return true;
        }
        if (value instanceof LibraryTreeItem) {
            showLibraryDoc(((LibraryTreeItem) value).title);
            showCard("study");
            return true;
        }
        if (value instanceof BookTreeItem) {
            String book = ((BookTreeItem) value).bookKey;
            if (safe(book).isEmpty() || !data.bible.containsKey(book)) return false;
            selectedBook = book;
            if (!data.getChapters(selectedBook).contains(selectedChapter)) {
                selectedChapter = data.getChapters(selectedBook).isEmpty() ? 1 : data.getChapters(selectedBook).iterator().next();
            }
            refreshBookCombo();
            showSelectedChapter(true);
            showCard("study");
            return true;
        }
        String sourceKey = sourceKeyForTreeItem(value);
        if (!safe(sourceKey).isEmpty()) {
            openSourceKey(sourceKey);
            showCard("study");
            return true;
        }
        return false;
    }

    private void openLibraryNodeFromBeginning(Object nodeValue) {
        openLibraryNode(nodeValue, false);
    }

    private void maybeShowLibraryContextMenu(MouseEvent e) {
        if (libraryTree == null || e == null || !e.isPopupTrigger()) return;
        TreePath path = libraryTree.getPathForLocation(e.getX(), e.getY());
        if (path == null) return;
        libraryTree.setSelectionPath(path);
        Object value = userObjectFromTreePath(path);
        boolean openable = canOpenLibraryNode(value);
        JPopupMenu menu = new JPopupMenu();
        JMenuItem open = new JMenuItem("Open");
        open.setEnabled(openable);
        open.addActionListener(a -> openLibraryNode(value, true));
        JMenuItem fromBeginning = new JMenuItem("Open from Beginning");
        fromBeginning.setEnabled(openable);
        fromBeginning.addActionListener(a -> openLibraryNodeFromBeginning(value));
        menu.add(open);
        menu.add(fromBeginning);
        menu.show(libraryTree, e.getX(), e.getY());
        e.consume();
    }

    private boolean canOpenLibraryNode(Object nodeValue) {
        Object value = nodeValue instanceof DefaultMutableTreeNode ? ((DefaultMutableTreeNode) nodeValue).getUserObject() : nodeValue;
        if (value instanceof TreeSourceItem) return !safe(((TreeSourceItem) value).sourceKey).isEmpty();
        if (value instanceof LibraryTreeItem) return data.findLibraryDoc(((LibraryTreeItem) value).title) != null;
        if (value instanceof BookTreeItem) return data.bible.containsKey(((BookTreeItem) value).bookKey);
        return !safe(sourceKeyForTreeItem(value)).isEmpty();
    }

    private String getBookmarkScopeKeyForCurrentLocation() {
        return bookmarkRowKeyForCurrentLocation();
    }

    private String getBookmarkScopeKeyForLibraryNode(Object nodeValue) {
        String sourceKey = sourceKeyForTreeItem(nodeValue);
        return safe(sourceKey).isEmpty() ? "" : canonicalSourceKey(sourceKey);
    }

    private StudyBookmark findBookmarkForScopeKey(String scopeKey) {
        if (safe(scopeKey).isEmpty() || currentProfile == null || currentProfile.bookmarks == null) return null;
        StudyBookmark newest = null;
        for (StudyBookmark b : currentProfile.bookmarks) {
            if (b == null) continue;
            String key = bookmarkRowKey(b);
            if (scopeKey.equals(key) && (newest == null || bookmarkTimestamp(b) > bookmarkTimestamp(newest))) newest = b;
        }
        return newest;
    }

    private String getLibraryDisplayName(Object rowValue) {
        Object value = rowValue;
        if (value instanceof DefaultMutableTreeNode) value = ((DefaultMutableTreeNode) value).getUserObject();
        if (value instanceof BookTreeItem) return getShortBookDisplayName(((BookTreeItem) value).bookKey);
        return cleanTreeItemText(value);
    }

    private String truncateForLibraryColumn(String text, int availableWidth, FontMetrics fm) {
        String value = text == null ? "" : text;
        if (fm == null || availableWidth <= 0 || fm.stringWidth(value) <= availableWidth) return value;
        String ellipsis = "...";
        int ellipsisWidth = fm.stringWidth(ellipsis);
        if (availableWidth <= ellipsisWidth) return ellipsis;
        int low = 0;
        int high = value.length();
        while (low < high) {
            int mid = (low + high + 1) / 2;
            if (fm.stringWidth(value.substring(0, mid)) + ellipsisWidth <= availableWidth) low = mid;
            else high = mid - 1;
        }
        return value.substring(0, Math.max(0, low)).trim() + ellipsis;
    }

    private boolean hasBookmarkForLibraryItem(TreePath path) {
        return getMostRecentBookmarkForLibraryItem(path) != null;
    }

    private StudyBookmark getMostRecentBookmarkForLibraryItem(TreePath path) {
        Object userObject = userObjectFromTreePath(path);
        String sourceKey = sourceKeyForTreeItem(userObject);
        return sourceKey == null ? null : getMostRecentBookmarkForSourceKey(sourceKey);
    }

    private void openBookmarkForLibraryItem(TreePath path) {
        StudyBookmark bookmark = getMostRecentBookmarkForLibraryItem(path);
        if (bookmark != null) openBookmark(bookmark);
    }

    private boolean hasBookmarkForSourceKey(String sourceKey) {
        return getMostRecentBookmarkForSourceKey(sourceKey) != null;
    }

    private StudyBookmark getMostRecentBookmarkForSourceKey(String sourceKey) {
        if (sourceKey == null || sourceKey.trim().isEmpty() || currentProfile == null || currentProfile.bookmarks == null) return null;
        StudyBookmark newest = null;
        for (StudyBookmark b : currentProfile.bookmarks) {
            if (bookmarkMatchesSourceKey(b, sourceKey) && (newest == null || bookmarkTimestamp(b) > bookmarkTimestamp(newest))) newest = b;
        }
        return newest;
    }

    private String sourceKeyForTreeItem(Object userObject) {
        Object value = userObject;
        if (value instanceof DefaultMutableTreeNode) value = ((DefaultMutableTreeNode) value).getUserObject();
        if (value instanceof TreeSourceItem) return emptyToNull(((TreeSourceItem) value).sourceKey);
        if (value instanceof LibraryTreeItem) return "LIBRARY:" + ((LibraryTreeItem) value).title;
        if (value instanceof BookTreeItem) return null;
        if (value instanceof String) {
            String s = ((String) value).trim();
            if (s.startsWith("BIBLE:") || s.startsWith("LIBRARY:")) return s;
        }
        String reflectedSourceKey = reflectedStringField(value, "sourceKey");
        if (reflectedSourceKey != null && !reflectedSourceKey.trim().isEmpty()) return reflectedSourceKey.trim();
        return null;
    }

    private Object userObjectFromTreePath(TreePath path) {
        if (path == null) return null;
        Object node = path.getLastPathComponent();
        return node instanceof DefaultMutableTreeNode ? ((DefaultMutableTreeNode) node).getUserObject() : node;
    }

    private boolean bookmarkMatchesSourceKey(StudyBookmark bookmark, String sourceKey) {
        if (bookmark == null || bookmark.sourceKey == null || sourceKey == null) return false;
        String bookmarkKey = canonicalSourceKey(bookmark.sourceKey);
        String treeKey = canonicalSourceKey(sourceKey);
        if (bookmarkKey.equals(treeKey)) return true;

        return false;
    }

    private String canonicalSourceKey(String sourceKey) {
        String key = safe(sourceKey).trim();
        if (!key.startsWith("BIBLE:")) return key;

        String rest = key.substring("BIBLE:".length()).trim();
        Matcher chapterMatcher = Pattern.compile("^(.+?)[\\s:]+(\\d+)$").matcher(rest);
        if (chapterMatcher.matches()) {
            return "BIBLE:" + chapterMatcher.group(1).trim() + ":" + chapterMatcher.group(2);
        }
        return "BIBLE:" + rest;
    }

    private boolean bibleSourceKeyHasChapter(String sourceKey) {
        if (sourceKey == null || !sourceKey.startsWith("BIBLE:")) return false;
        String rest = sourceKey.substring("BIBLE:".length()).trim();
        return Pattern.compile("^.+[\\s:]+\\d+$").matcher(rest).matches();
    }

    private String emptyToNull(String value) {
        return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    private String reflectedStringField(Object value, String fieldName) {
        if (value == null) return null;
        try {
            java.lang.reflect.Field field = value.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            Object fieldValue = field.get(value);
            return fieldValue == null ? null : fieldValue.toString();
        } catch (ReflectiveOperationException | RuntimeException ignored) {
            return null;
        }
    }

    private Integer chapterNumberFromTreePathPart(Object pathPart) {
        String s = cleanTreeItemText(pathPart);
        if (s.startsWith("Chapter ")) s = s.substring("Chapter ".length());
        s = s.trim().split("\\s+")[0];
        try { return Integer.parseInt(s); } catch (Exception ignored) { return null; }
    }

    private long bookmarkTimestamp(StudyBookmark b) {
        return b == null ? 0L : Math.max(b.createdAt, b.updatedAt);
    }

    private String cleanTreeItemText(Object value) {
        if (value instanceof BookTreeItem) return ((BookTreeItem) value).displayName;
        if (value instanceof LibraryTreeItem) return ((LibraryTreeItem) value).title;
        if (value instanceof TreeSourceItem) return ((TreeSourceItem) value).displayName;
        return value == null ? "" : value.toString();
    }

    private List<String> orderedBooks() {
        List<String> list = new ArrayList<>(data.bible.keySet());
        Map<String, Integer> order = bibleBookOrder();
        list.sort(Comparator.comparingInt((String b) -> order.getOrDefault(displayBibleBookName(b).toLowerCase(Locale.ROOT), 999)).thenComparing(b -> displayBibleBookName(b)).thenComparing(b -> b));
        return list;
    }

    private String bookKeyFromTreePathPart(Object pathPart) {
        Object value = pathPart;
        if (pathPart instanceof DefaultMutableTreeNode) value = ((DefaultMutableTreeNode) pathPart).getUserObject();
        if (value instanceof BookTreeItem) return ((BookTreeItem) value).bookKey;
        return value == null ? "" : value.toString();
    }

    private String bookKeyFromComboItem(Object item) {
        if (item instanceof BookTreeItem) return ((BookTreeItem) item).bookKey;
        return item == null ? "" : item.toString();
    }

    private String libraryTitleFromTreePathPart(Object pathPart) {
        Object value = pathPart;
        if (pathPart instanceof DefaultMutableTreeNode) value = ((DefaultMutableTreeNode) pathPart).getUserObject();
        if (value instanceof LibraryTreeItem) return ((LibraryTreeItem) value).title;
        return value == null ? "" : value.toString();
    }

    private String getShortBookDisplayName(String bookName) {
        return displayBibleBookName(bookName);
    }

    private String displayBibleBookName(String importedName) {
        String original = safe(importedName).trim();
        if (original.isEmpty()) return original;

        Map<String, Integer> order = bibleBookOrder();
        Map<String, String> canonicalByLower = new HashMap<>();
        for (String canonical : order.keySet()) canonicalByLower.put(canonical, toTitleBookName(canonical));

        String normalized = normalizeBookDisplayName(original);
        String direct = canonicalByLower.get(normalized);
        if (direct != null) return direct;

        if (normalized.equals("revelation to john") || normalized.equals("revelation of john")
                || normalized.equals("the revelation to john") || normalized.equals("the revelation of john")
                || normalized.equals("apocalypse of john")) return "Revelation";

        String candidate = normalized;
        if (candidate.startsWith("the ")) candidate = candidate.substring(4).trim();
        if (candidate.startsWith("gospel according to ")) candidate = candidate.substring("gospel according to ".length()).trim();
        if (candidate.startsWith("holy gospel according to ")) candidate = candidate.substring("holy gospel according to ".length()).trim();
        if (candidate.startsWith("book of ")) candidate = candidate.substring("book of ".length()).trim();
        if (candidate.startsWith("song of songs")) candidate = "song of solomon";
        if (candidate.startsWith("canticle of canticles")) candidate = "song of solomon";

        candidate = candidate.replaceFirst("^(pauls|paul s) letter to (the )?", "");
        candidate = candidate.replaceFirst("^(letter|epistle) of paul to (the )?", "");
        candidate = candidate.replaceFirst("^(letter|epistle) to (the )?", "");
        candidate = candidate.replaceFirst("^(letter|epistle) of (the )?", "");
        candidate = candidate.replaceFirst("^general (letter|epistle) of (the )?", "");
        candidate = candidate.replaceFirst("^(1|2|3) (letter|epistle) (to|of) (the )?", "$1 ");
        if (candidate.startsWith("the ")) candidate = candidate.substring(4).trim();

        String alias = canonicalBookAlias(candidate);
        if (!alias.isEmpty()) return alias;

        for (String lower : canonicalByLower.keySet()) {
            String canonical = canonicalByLower.get(lower);
            if (candidate.equals(lower) || candidate.equals("the " + lower) || candidate.endsWith(" " + lower)) return canonical;
        }
        return original;
    }

    private String normalizeBookDisplayName(String name) {
        String normalized = name.toLowerCase(Locale.ROOT)
                .replace('’', '\'')
                .replaceAll("[^a-z0-9']+", " ")
                .replaceAll("\\s+", " ")
                .trim();
        normalized = normalized.replace("'", " ").replaceAll("\\s+", " ").trim();
        normalized = normalized.replaceFirst("^first ", "1 ")
                .replaceFirst("^second ", "2 ")
                .replaceFirst("^third ", "3 ")
                .replace(" first letter ", " 1 letter ")
                .replace(" second letter ", " 2 letter ")
                .replace(" third letter ", " 3 letter ")
                .replace(" first epistle ", " 1 epistle ")
                .replace(" second epistle ", " 2 epistle ")
                .replace(" third epistle ", " 3 epistle ");
        return normalized.replaceAll("\\s+", " ").trim();
    }

    private String canonicalBookAlias(String candidate) {
        Map<String, String> aliases = new HashMap<>();
        aliases.put("psalm", "Psalms");
        aliases.put("psalms", "Psalms");
        aliases.put("song of songs", "Song of Solomon");
        aliases.put("song of solomon", "Song of Solomon");
        aliases.put("canticles", "Song of Solomon");
        aliases.put("acts of apostles", "Acts");
        aliases.put("acts of the apostles", "Acts");
        aliases.put("apocalypse", "Revelation");
        aliases.put("apocalypse of john", "Revelation");
        aliases.put("revelation", "Revelation");
        aliases.put("the apocalypse", "Revelation");
        return aliases.getOrDefault(candidate, "");
    }

    private String toTitleBookName(String lowerCanonical) {
        for (String book : bibleBookOrderNames()) {
            if (book.toLowerCase(Locale.ROOT).equals(lowerCanonical)) return book;
        }
        return lowerCanonical;
    }

    private Map<String, Integer> bibleBookOrder() {
        String[] a = bibleBookOrderNames();
        Map<String, Integer> m = new HashMap<>();
        for (int i = 0; i < a.length; i++) m.put(a[i].toLowerCase(Locale.ROOT), i + 1);
        return m;
    }

    private String[] bibleBookOrderNames() {
        return new String[]{
                "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy", "Joshua", "Judges", "Ruth",
                "1 Samuel", "2 Samuel", "1 Kings", "2 Kings", "1 Chronicles", "2 Chronicles", "Ezra", "Nehemiah", "Esther",
                "Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Solomon", "Isaiah", "Jeremiah", "Lamentations", "Ezekiel", "Daniel",
                "Hosea", "Joel", "Amos", "Obadiah", "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah", "Haggai", "Zechariah", "Malachi",
                "Matthew", "Mark", "Luke", "John", "Acts", "Romans", "1 Corinthians", "2 Corinthians", "Galatians", "Ephesians", "Philippians", "Colossians",
                "1 Thessalonians", "2 Thessalonians", "1 Timothy", "2 Timothy", "Titus", "Philemon", "Hebrews", "James", "1 Peter", "2 Peter",
                "1 John", "2 John", "3 John", "Jude", "Revelation"
        };
    }

    private void refreshBookCombo() {
        if (bookCombo == null) return;
        refreshingUi = true;
        try {
            Object selected = bookCombo.getSelectedItem();
            bookCombo.removeAllItems();
            for (String b : orderedBooks()) bookCombo.addItem(new BookTreeItem(b, displayBibleBookName(b)));
            if (selectedBook == null || selectedBook.isEmpty() || !data.bible.containsKey(selectedBook)) {
                String selectedKey = bookKeyFromComboItem(selected);
                if (data.bible.containsKey(selectedKey)) selectedBook = selectedKey;
                else if (bookCombo.getItemCount() > 0) selectedBook = bookCombo.getItemAt(0).bookKey;
            }
            bookCombo.setSelectedItem(new BookTreeItem(selectedBook, displayBibleBookName(selectedBook)));
            refreshChapterCombo();
        } finally {
            refreshingUi = false;
        }
    }

    private void refreshChapterCombo() {
        if (chapterCombo == null) return;
        refreshingUi = true;
        try {
            chapterCombo.removeAllItems();
            if (selectedBook == null || selectedBook.isEmpty()) return;
            for (Integer ch : data.getChapters(selectedBook)) chapterCombo.addItem(ch);
            if (!data.getChapters(selectedBook).contains(selectedChapter) && chapterCombo.getItemCount() > 0) {
                selectedChapter = chapterCombo.getItemAt(0);
            }
            chapterCombo.setSelectedItem(selectedChapter);
        } finally {
            refreshingUi = false;
        }
    }

    private void installReaderShortcuts() {
        InputMap input = readerPane.getInputMap(JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
        ActionMap actions = readerPane.getActionMap();
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_LEFT, InputEvent.CTRL_DOWN_MASK), "previousChapter");
        actions.put("previousChapter", new AbstractAction() {
            public void actionPerformed(ActionEvent e) { previousChapter(); }
        });
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_RIGHT, InputEvent.CTRL_DOWN_MASK), "nextChapter");
        actions.put("nextChapter", new AbstractAction() {
            public void actionPerformed(ActionEvent e) { nextChapter(); }
        });
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "exitReadingMode");
        actions.put("exitReadingMode", new AbstractAction() {
            public void actionPerformed(ActionEvent e) { if (readingMode) exitReadingMode(); }
        });
    }

    private void previousChapter() {
        if (data == null || data.bible == null || data.bible.isEmpty() || selectedBook == null || selectedBook.isEmpty()) return;
        List<String> books = orderedBooks();
        int bookIndex = books.indexOf(selectedBook);
        if (bookIndex < 0) return;
        TreeSet<Integer> chapters = new TreeSet<>(data.getChapters(selectedBook));
        Integer prev = chapters.lower(selectedChapter);
        if (prev != null) {
            selectedChapter = prev;
        } else if (bookIndex > 0) {
            selectedBook = books.get(bookIndex - 1);
            TreeSet<Integer> previousBookChapters = new TreeSet<>(data.getChapters(selectedBook));
            if (previousBookChapters.isEmpty()) return;
            selectedChapter = previousBookChapters.last();
        } else {
            return;
        }
        refreshBookCombo();
        showSelectedChapter(true);
    }

    private void nextChapter() {
        if (data == null || data.bible == null || data.bible.isEmpty() || selectedBook == null || selectedBook.isEmpty()) return;
        List<String> books = orderedBooks();
        int bookIndex = books.indexOf(selectedBook);
        if (bookIndex < 0) return;
        TreeSet<Integer> chapters = new TreeSet<>(data.getChapters(selectedBook));
        Integer next = chapters.higher(selectedChapter);
        if (next != null) {
            selectedChapter = next;
        } else if (bookIndex < books.size() - 1) {
            selectedBook = books.get(bookIndex + 1);
            TreeSet<Integer> nextBookChapters = new TreeSet<>(data.getChapters(selectedBook));
            if (nextBookChapters.isEmpty()) return;
            selectedChapter = nextBookChapters.first();
        } else {
            return;
        }
        refreshBookCombo();
        showSelectedChapter(true);
    }



    private void toggleReadingMode() {
        if (readingMode) exitReadingMode(); else enterReadingMode();
    }

    private void focusBestSearchField() {
        if ("study".equals(activeCardName) && sideSearchField != null) {
            sideSearchField.requestFocusInWindow();
            sideSearchField.selectAll();
        } else if (searchField != null) {
            showCard("search");
            searchField.requestFocusInWindow();
            searchField.selectAll();
        }
    }

    private void showCommandPalette() {
        final JDialog dialog = new JDialog(this, "Command Palette", true);
        dialog.setLayout(new BorderLayout(8, 8));
        ((JComponent) dialog.getContentPane()).setBorder(new EmptyBorder(12, 12, 12, 12));
        JTextField field = new JTextField();
        DefaultListModel<CommandPaletteItem> model = new DefaultListModel<>();
        JList<CommandPaletteItem> list = new JList<>(model);
        list.setVisibleRowCount(10);
        Runnable refresh = () -> refreshCommandPaletteResults(field.getText(), model);
        field.getDocument().addDocumentListener(new SimpleDocumentListener(refresh));
        field.addActionListener(e -> activateCommandPaletteSelection(list, field.getText(), dialog));
        list.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 2) activateCommandPaletteSelection(list, field.getText(), dialog);
            }
        });
        InputMap input = dialog.getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
        ActionMap actions = dialog.getRootPane().getActionMap();
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "closePalette");
        actions.put("closePalette", new AbstractAction() { public void actionPerformed(ActionEvent e) { dialog.dispose(); }});
        dialog.add(new JLabel("Type a command or Bible reference:"), BorderLayout.NORTH);
        dialog.add(field, BorderLayout.CENTER);
        dialog.add(new JScrollPane(list), BorderLayout.SOUTH);
        dialog.setSize(460, 360);
        dialog.setLocationRelativeTo(this);
        refresh.run();
        SwingUtilities.invokeLater(() -> field.requestFocusInWindow());
        dialog.setVisible(true);
    }

    private void refreshCommandPaletteResults(String raw, DefaultListModel<CommandPaletteItem> model) {
        model.clear();
        String q = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
        addCommandIfMatches(model, q, "Study", () -> showCard("study"));
        addCommandIfMatches(model, q, "Search", () -> showCard("search"));
        addCommandIfMatches(model, q, "Greek Search", () -> showCard("greekSearch"));
        addCommandIfMatches(model, q, "Memory Verses", () -> { refreshMemoryVerses(); showCard("memory"); });
        addCommandIfMatches(model, q, "Study Projects", () -> { refreshStudyProjects(); showCard("studyProjects"); });
        addCommandIfMatches(model, q, "Recent Notes", () -> { refreshRecentNotes(); showCard("recent"); });
        addCommandIfMatches(model, q, "Categories", () -> { refreshCategories(); showCard("categories"); });
        addCommandIfMatches(model, q, "Questions", () -> { refreshQuestions(); showCard("questions"); });
        addCommandIfMatches(model, q, "Topic Pages", () -> { refreshTopicPages(); showCard("topicPages"); });
        addCommandIfMatches(model, q, "Bookmarks", this::showBookmarksDialog);
        addCommandIfMatches(model, q, "Reading Mode", this::toggleReadingMode);
        addCommandIfMatches(model, q, "Import", () -> showCard("import"));
        addCommandIfMatches(model, q, "Backup", this::backupNow);
        addCommandIfMatches(model, q, "Export", this::exportNotes);
        if (!q.isEmpty() && looksLikeBibleReference(raw)) {
            model.insertElementAt(new CommandPaletteItem("Go to " + raw.trim(), () -> openReference(raw.trim(), true)), 0);
        }
        if (model.isEmpty()) model.addElement(new CommandPaletteItem("No matching commands", () -> {}));
    }

    private void addCommandIfMatches(DefaultListModel<CommandPaletteItem> model, String q, String label, Runnable action) {
        if (q == null || q.isEmpty() || label.toLowerCase(Locale.ROOT).contains(q)) model.addElement(new CommandPaletteItem(label, action));
    }

    private void activateCommandPaletteSelection(JList<CommandPaletteItem> list, String raw, JDialog dialog) {
        CommandPaletteItem item = list.getSelectedValue();
        if (item == null && list.getModel().getSize() > 0) item = list.getModel().getElementAt(0);
        if (item == null) return;
        dialog.dispose();
        item.action.run();
    }

    private boolean looksLikeBibleReference(String raw) {
        if (raw == null) return false;
        return parseBibleReference(raw.trim()) != null || parseChapterRef(raw.trim()) != null || parseRef(raw.trim()) != null || parseBibleReferenceOrRange(raw.trim()) != null;
    }

    private void installReferenceAutocomplete() {
        if (goToReferenceField == null) return;
        referenceSuggestionModel = new DefaultListModel<>();
        referenceSuggestionList = new JList<>(referenceSuggestionModel);
        referenceSuggestionList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        referenceSuggestionList.setVisibleRowCount(6);
        referenceSuggestionList.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        referenceSuggestionList.setBorder(new EmptyBorder(2, 2, 2, 2));
        referenceSuggestionList.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e)) {
                    String suggestion = referenceSuggestionList.getSelectedValue();
                    if (suggestion != null) acceptReferenceSuggestion(suggestion);
                }
            }
        });

        referenceSuggestionPopup = new JPopupMenu();
        referenceSuggestionPopup.setFocusable(false);
        referenceSuggestionPopup.setBorder(new CompoundBorder(new LineBorder(modernBorder), new EmptyBorder(2, 2, 2, 2)));
        JScrollPane scroll = new JScrollPane(referenceSuggestionList);
        scroll.setBorder(null);
        scroll.setPreferredSize(new Dimension(260, 138));
        referenceSuggestionPopup.add(scroll);

        goToReferenceField.getDocument().addDocumentListener(new SimpleDocumentListener(this::updateReferenceSuggestions));
        goToReferenceField.addFocusListener(new FocusAdapter() {
            public void focusLost(FocusEvent e) {
                SwingUtilities.invokeLater(() -> {
                    if (referenceSuggestionPopup != null && !referenceSuggestionPopup.isFocusOwner()) referenceSuggestionPopup.setVisible(false);
                });
            }
        });

        InputMap input = goToReferenceField.getInputMap(JComponent.WHEN_FOCUSED);
        ActionMap actions = goToReferenceField.getActionMap();
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, 0), "acceptReferenceSuggestion");
        actions.put("acceptReferenceSuggestion", new AbstractAction() {
            public void actionPerformed(ActionEvent e) {
                if (!referenceTextHasNumbers(goToReferenceField == null ? "" : goToReferenceField.getText())
                        && hasVisibleReferenceSuggestion()) {
                    acceptTopReferenceSuggestionOnly();
                    return;
                }
                goToReferenceFromBox();
            }
        });
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_DOWN, 0), "nextReferenceSuggestion");
        actions.put("nextReferenceSuggestion", new AbstractAction() {
            public void actionPerformed(ActionEvent e) { moveReferenceSuggestionSelection(1); }
        });
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_UP, 0), "previousReferenceSuggestion");
        actions.put("previousReferenceSuggestion", new AbstractAction() {
            public void actionPerformed(ActionEvent e) { moveReferenceSuggestionSelection(-1); }
        });
        input.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "hideReferenceSuggestions");
        actions.put("hideReferenceSuggestions", new AbstractAction() {
            public void actionPerformed(ActionEvent e) { if (referenceSuggestionPopup != null) referenceSuggestionPopup.setVisible(false); }
        });
    }

    private void moveReferenceSuggestionSelection(int delta) {
        if (referenceSuggestionModel == null || referenceSuggestionModel.isEmpty()) {
            updateReferenceSuggestions();
            return;
        }
        if (referenceSuggestionPopup != null && !referenceSuggestionPopup.isVisible()) showReferenceSuggestionPopup();
        int idx = referenceSuggestionList.getSelectedIndex();
        if (idx < 0) idx = delta > 0 ? -1 : 0;
        idx = Math.max(0, Math.min(referenceSuggestionModel.getSize() - 1, idx + delta));
        referenceSuggestionList.setSelectedIndex(idx);
        referenceSuggestionList.ensureIndexIsVisible(idx);
    }

    private void updateReferenceSuggestions() {
        if (referenceSuggestionModel == null || goToReferenceField == null) return;
        java.util.List<String> suggestions = getReferenceSuggestions(goToReferenceField.getText());
        referenceSuggestionModel.clear();
        for (String suggestion : suggestions) referenceSuggestionModel.addElement(suggestion);
        if (!suggestions.isEmpty()) {
            referenceSuggestionList.setSelectedIndex(0);
            showReferenceSuggestionPopup();
        } else if (referenceSuggestionPopup != null) {
            referenceSuggestionPopup.setVisible(false);
        }
    }

    private void showReferenceSuggestionPopup() {
        if (referenceSuggestionPopup == null || goToReferenceField == null || referenceSuggestionModel == null || referenceSuggestionModel.isEmpty()) return;
        int rowHeight = Math.max(22, referenceSuggestionList.getFixedCellHeight() > 0 ? referenceSuggestionList.getFixedCellHeight() : goToReferenceField.getFontMetrics(goToReferenceField.getFont()).getHeight() + 6);
        int height = Math.min(160, Math.max(28, referenceSuggestionModel.getSize() * rowHeight + 6));
        int width = Math.max(goToReferenceField.getWidth(), 260);
        ((JScrollPane) referenceSuggestionPopup.getComponent(0)).setPreferredSize(new Dimension(width, height));
        referenceSuggestionPopup.pack();
        try {
            referenceSuggestionPopup.show(goToReferenceField, 0, goToReferenceField.getHeight());
        } catch (IllegalComponentStateException ignored) {}
    }

    private java.util.List<String> getReferenceSuggestions(String typedText) {
        java.util.List<String> out = new ArrayList<>();
        if (data == null || data.bible == null || data.bible.isEmpty()) return out;
        String raw = safe(typedText).trim();
        if (raw.isEmpty()) return out;
        String normalized = normalizeReferenceInput(raw);
        if (normalized.isEmpty()) return out;

        String suffix = "";
        String bookQuery = normalized;
        Matcher suffixMatcher = Pattern.compile("^(.*?)(?:\\s+)(\\d+(?::\\d*)?)$").matcher(normalized);
        if (suffixMatcher.matches() && !suffixMatcher.group(1).trim().isEmpty()) {
            bookQuery = suffixMatcher.group(1).trim();
            suffix = " " + suffixMatcher.group(2).trim();
        }
        String compactQuery = bookQuery.replace(" ", "");
        if (bookQuery.isEmpty()) return out;

        Map<String, String> aliases = buildBookAliasMap();
        Map<String, Integer> bestScoreByBook = new HashMap<>();
        for (Map.Entry<String, String> e : aliases.entrySet()) {
            String alias = e.getKey();
            String compactAlias = alias.replace(" ", "");
            String display = displayBibleBookName(e.getValue());
            String displayNorm = normalizeReferenceBookAlias(display);
            int score = 0;
            if (alias.equals(bookQuery) || compactAlias.equals(compactQuery)) score = 1000;
            else if (displayNorm.startsWith(bookQuery)) score = 900 - Math.max(0, displayNorm.length() - bookQuery.length());
            else if (alias.startsWith(bookQuery) || compactAlias.startsWith(compactQuery)) score = 820 - Math.max(0, alias.length() - bookQuery.length());
            else if (displayNorm.contains(bookQuery)) score = 620 - displayNorm.indexOf(bookQuery);
            else if (alias.contains(bookQuery) || compactAlias.contains(compactQuery)) score = 560 - Math.max(alias.indexOf(bookQuery), 0);
            if (score > 0) bestScoreByBook.merge(e.getValue(), score, Math::max);
        }

        java.util.List<String> books = new ArrayList<>(bestScoreByBook.keySet());
        Map<String, Integer> order = bibleBookOrder();
        books.sort((a, b) -> {
            int score = Integer.compare(bestScoreByBook.getOrDefault(b, 0), bestScoreByBook.getOrDefault(a, 0));
            if (score != 0) return score;
            return Integer.compare(order.getOrDefault(displayBibleBookName(a).toLowerCase(Locale.ROOT), 999), order.getOrDefault(displayBibleBookName(b).toLowerCase(Locale.ROOT), 999));
        });
        for (String bookKey : books) {
            if (out.size() >= 8) break;
            out.add(displayBibleBookName(bookKey) + suffix);
        }
        return out;
    }

    private boolean referenceTextHasNumbers(String text) {
        return text != null && text.matches(".*\\d+.*");
    }

    private boolean hasVisibleReferenceSuggestion() {
        return referenceSuggestionPopup != null && referenceSuggestionPopup.isVisible()
                && referenceSuggestionModel != null && referenceSuggestionModel.getSize() > 0;
    }

    private String topReferenceSuggestion() {
        String suggestion = referenceSuggestionList == null ? null : referenceSuggestionList.getSelectedValue();
        if (suggestion == null && referenceSuggestionModel != null && referenceSuggestionModel.getSize() > 0) {
            suggestion = referenceSuggestionModel.getElementAt(0);
        }
        return suggestion;
    }

    private void acceptTopReferenceSuggestionOnly() {
        acceptReferenceSuggestionOnly(topReferenceSuggestion());
    }

    private void acceptReferenceSuggestionOnly(String suggestion) {
        if (suggestion == null || suggestion.trim().isEmpty()) return;
        if (goToReferenceField != null) {
            goToReferenceField.setText(suggestion.trim());
            goToReferenceField.setCaretPosition(goToReferenceField.getText().length());
        }
        if (referenceSuggestionPopup != null) referenceSuggestionPopup.setVisible(false);
    }

    private void acceptReferenceSuggestion(String suggestion) {
        if (suggestion == null || suggestion.trim().isEmpty()) return;
        acceptReferenceSuggestionOnly(suggestion);
        openReference(suggestion, true);
    }

    private void goToReferenceFromBox() {
        String raw = goToReferenceField == null ? "" : goToReferenceField.getText().trim();
        if (raw.isEmpty()) return;
        openReference(raw, true);
    }

    private boolean openReference(String raw, boolean showMessage) {
        if (raw == null || raw.trim().isEmpty()) return false;
        String target = raw.trim();
        ParsedReference parsed = parseBibleReference(target);
        if (parsed != null) {
            goToBibleReference(parsed.bookKey, parsed.chapter, parsed.verse);
            return true;
        }

        ChapterRef cr = parseChapterRef(target);
        PassageRef pr = parseBibleReferenceOrRange(target);
        RefParts rp = parseRef(target);
        if (cr != null) {
            goToBibleReference(cr.book, cr.chapter, null);
            return true;
        }
        if (pr != null) {
            goToBibleReference(pr.book, pr.chapter, pr.startVerse);
            return true;
        }
        if (rp != null && data.bible.containsKey(rp.book) && data.getChapters(rp.book).contains(rp.chapter)) {
            goToBibleReference(rp.book, rp.chapter, rp.verse);
            return true;
        }
        String message = "Reference not found: " + raw + "\nTry formats like Romans 3:23, Rom 3, 1 John 1:9, Gen 1, or John 3:16.";
        setStatusMessage(message.replace("\n", " "));
        if (showMessage) JOptionPane.showMessageDialog(this, message);
        return false;
    }

    private NavigationLocation currentNavigationLocation() {
        int caret = readerPane == null ? 0 : readerPane.getCaretPosition();
        int ss = readerPane == null ? -1 : readerPane.getSelectionStart();
        int se = readerPane == null ? -1 : readerPane.getSelectionEnd();
        return new NavigationLocation(currentSourceKey, currentSourceTitle, selectedBook, selectedChapter, caret, ss, se);
    }

    private void trackReaderLocation() {
        if (restoringHistory || currentSourceKey == null || currentSourceKey.trim().isEmpty()) return;
        NavigationLocation loc = currentNavigationLocation();
        if (backHistory.isEmpty() || !backHistory.get(backHistory.size() - 1).samePlace(loc)) backHistory.add(loc);
        if (backHistory.size() > 80) backHistory.remove(0);
        forwardHistory.clear();
        addRecentlyOpened(loc);
        updateHistoryButtons();
    }

    private void goBackLocation() {
        if (backHistory.size() < 2) return;
        NavigationLocation current = backHistory.remove(backHistory.size() - 1);
        forwardHistory.add(current);
        restoreNavigationLocation(backHistory.get(backHistory.size() - 1));
        updateHistoryButtons();
    }

    private void goForwardLocation() {
        if (forwardHistory.isEmpty()) return;
        NavigationLocation loc = forwardHistory.remove(forwardHistory.size() - 1);
        backHistory.add(loc);
        restoreNavigationLocation(loc);
        updateHistoryButtons();
    }

    private void restoreNavigationLocation(NavigationLocation loc) {
        if (loc == null) return;
        restoringHistory = true;
        try {
            if (safe(loc.sourceKey).startsWith("BIBLE:") || safe(loc.sourceKey).startsWith("LIBRARY:")) openSourceKey(loc.sourceKey);
            else if (!safe(loc.selectedBook).isEmpty()) { selectedBook = loc.selectedBook; selectedChapter = loc.selectedChapter; refreshBookCombo(); showSelectedChapter(false); }
            final int caret = loc.caretPosition, ss = loc.selectionStart, se = loc.selectionEnd;
            SwingUtilities.invokeLater(() -> {
                if (ss >= 0 && se > ss) safeSelect(ss, se); else moveReaderCaret(caret);
                showCard("study");
            });
        } finally { restoringHistory = false; }
    }

    private void updateHistoryButtons() {
        if (backButton != null) backButton.setEnabled(backHistory.size() > 1);
        if (forwardButton != null) forwardButton.setEnabled(!forwardHistory.isEmpty());
    }

    private void addRecentlyOpened(NavigationLocation loc) {
        if (currentProfile == null || loc == null || safe(loc.sourceKey).isEmpty()) return;
        if (currentProfile.recentlyOpened == null) currentProfile.recentlyOpened = new ArrayList<>();
        RecentLocation r = new RecentLocation(loc.sourceKey, safe(loc.sourceTitle).isEmpty() ? loc.sourceKey : loc.sourceTitle, loc.selectedBook, loc.selectedChapter, loc.caretPosition, loc.selectionStart, loc.selectionEnd);
        currentProfile.recentlyOpened.removeIf(x -> x != null && safe(x.sourceKey).equals(r.sourceKey) && x.caretPosition == r.caretPosition);
        currentProfile.recentlyOpened.add(0, r);
        while (currentProfile.recentlyOpened.size() > 8) currentProfile.recentlyOpened.remove(currentProfile.recentlyOpened.size() - 1);
        refreshRecentlyOpened();
        saveData();
    }

    private void refreshRecentlyOpened() {
        if (recentlyOpenedModel == null) return;
        recentlyOpenedModel.clear();
        if (currentProfile == null || currentProfile.recentlyOpened == null || currentProfile.recentlyOpened.isEmpty()) {
            recentlyOpenedModel.addElement(new RecentLocation("", "No recent locations yet", "", 1, 0, -1, -1));
            return;
        }
        for (RecentLocation r : currentProfile.recentlyOpened) recentlyOpenedModel.addElement(r);
    }

    private void openRecentlyOpenedSelection() {
        RecentLocation r = recentlyOpenedList == null ? null : recentlyOpenedList.getSelectedValue();
        if (r == null || safe(r.sourceKey).isEmpty()) return;
        restoreNavigationLocation(new NavigationLocation(r.sourceKey, r.sourceTitle, r.selectedBook, r.selectedChapter, r.caretPosition, r.selectionStart, r.selectionEnd));
    }

    private void repairRecentLocation(RecentLocation r) {
        if (r == null) return;
        if (r.sourceKey == null) r.sourceKey = "";
        if (r.sourceTitle == null) r.sourceTitle = r.sourceKey;
        if (r.selectedBook == null) r.selectedBook = "";
        if (r.openedAt <= 0L) r.openedAt = System.currentTimeMillis();
    }

    private void enterReadingMode() {
        if (readingMode) return;
        readingMode = true;
        if (mainStudySplit != null) normalMainDividerLocation = mainStudySplit.getDividerLocation();
        if (centerRightSplit != null) normalCenterRightDividerLocation = centerRightSplit.getDividerLocation();
        normalReaderFontSize = readerPane == null ? 17 : readerPane.getFont().getSize();
        if (exitReadingModeButton != null) exitReadingModeButton.setVisible(true);
        if (mainStudySplit != null) {
            Component left = mainStudySplit.getLeftComponent();
            if (left != null) left.setVisible(false);
            mainStudySplit.setDividerSize(0);
            mainStudySplit.setDividerLocation(0);
        }
        if (centerRightSplit != null) {
            Component right = centerRightSplit.getRightComponent();
            if (right != null) right.setVisible(false);
            centerRightSplit.setDividerSize(0);
            centerRightSplit.setDividerLocation(1.0);
        }
        setReaderBodyFontSize(normalReaderFontSize + 3);
        showCard("study");
        revalidate();
        repaint();
    }

    private void exitReadingMode() {
        if (!readingMode) return;
        readingMode = false;
        if (exitReadingModeButton != null) exitReadingModeButton.setVisible(false);
        if (mainStudySplit != null) {
            Component left = mainStudySplit.getLeftComponent();
            if (left != null) left.setVisible(true);
            mainStudySplit.setDividerSize(7);
            if (normalMainDividerLocation >= 0) mainStudySplit.setDividerLocation(normalMainDividerLocation);
        }
        if (centerRightSplit != null) {
            Component right = centerRightSplit.getRightComponent();
            if (right != null) right.setVisible(true);
            centerRightSplit.setDividerSize(7);
            if (normalCenterRightDividerLocation >= 0) centerRightSplit.setDividerLocation(normalCenterRightDividerLocation);
            clampCenterRightDivider(true);
        }
        setReaderBodyFontSize(normalReaderFontSize);
        revalidate();
        repaint();
    }

    private void toggleMarginNotesMode() {
        if (readingMode) return;
        showRightSidebarMode("margin");
        if (centerRightSplit != null) {
            Component right = centerRightSplit.getRightComponent();
            if (right != null) right.setVisible(true);
            centerRightSplit.setDividerSize(7);
            clampCenterRightDivider(false);
            centerRightSplit.revalidate();
            centerRightSplit.repaint();
        }
    }

    private JPanel buildMarginNotesPanel() {
        marginNotesPanel = new JPanel(new BorderLayout(6, 6));
        marginNotesPanel.setMinimumSize(new Dimension(0, 0));
        marginNotesPanel.setBackground(panelBg);
        marginNotesPanel.setBorder(new EmptyBorder(0, 0, 0, 0));

        JPanel top = new JPanel(new FlowLayout(FlowLayout.RIGHT, 0, 0));
        top.setOpaque(false);
        JButton refresh = blackButton("↻");
        refresh.setMargin(new Insets(2, 7, 2, 7));
        refresh.setToolTipText("Refresh margin notes for the current chapter or source.");
        refresh.addActionListener(e -> refreshMarginNotesPanel());
        top.add(refresh);

        marginNotesBody = new WidthTrackingPanel();
        marginNotesBody.setLayout(new BoxLayout(marginNotesBody, BoxLayout.Y_AXIS));
        marginNotesBody.setBackground(cream);
        marginNotesBody.setBorder(new EmptyBorder(8, 8, 8, 8));

        marginNotesScroll = new JScrollPane(marginNotesBody);
        marginNotesScroll.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
        marginNotesScroll.getVerticalScrollBar().setUnitIncrement(16);
        marginNotesScroll.setBorder(new RoundedBorder(modernBorder, 12, new Insets(1, 1, 1, 1)));
        marginNotesScroll.getViewport().setBackground(cream);

        marginNotesPanel.add(top, BorderLayout.NORTH);
        marginNotesPanel.add(marginNotesScroll, BorderLayout.CENTER);
        return marginNotesPanel;
    }

    private void refreshMarginNotesPanel() {
        if (!"margin".equals(rightSidebarMode) || marginNotesBody == null) return;
        marginNotesBody.removeAll();
        java.util.List<ChapterNote> chapterNotes = currentChapterNotesForReader();
        java.util.List<TextAnnotation> annotations = getAnnotationsForCurrentReaderLocation();
        if (chapterNotes.isEmpty() && annotations.isEmpty()) {
            JTextArea empty = readonlyArea();
            empty.setBackground(cream);
            empty.setText("No notes or questions found for this chapter yet. Add a chapter note with the pencil button, or add a note, question, or category note from selected reader text.");
            marginNotesBody.add(empty);
        } else {
            boolean first = true;
            for (ChapterNote n : chapterNotes) {
                if (!first) marginNotesBody.add(Box.createVerticalStrut(7));
                marginNotesBody.add(buildMarginChapterNoteCard(n));
                first = false;
            }
            for (TextAnnotation a : annotations) {
                if (!first) marginNotesBody.add(Box.createVerticalStrut(7));
                marginNotesBody.add(buildMarginNoteCard(a, expandedMarginNoteIds.contains(safe(a.id))));
                first = false;
            }
        }
        marginNotesBody.revalidate();
        marginNotesBody.repaint();
    }

    private java.util.List<TextAnnotation> getAnnotationsForCurrentReaderLocation() {
        java.util.List<TextAnnotation> list = new ArrayList<>();
        if (currentProfile == null || safe(currentSourceKey).isEmpty()) return list;
        for (TextAnnotation a : currentProfile.annotations) {
            if (a == null || !currentSourceKey.equals(a.sourceKey)) continue;
            if (isChapterLevelNote(a)) continue;
            boolean hasNoteText = !safe(a.note).isEmpty();
            boolean isChapterNote = "Note".equals(a.type) || "Question".equals(a.type);
            boolean isCategoryWithNote = "Category".equals(a.type) && hasNoteText;
            if (!(isChapterNote || isCategoryWithNote || hasNoteText)) continue;
            list.add(a);
        }
        list.sort(Comparator
                .comparingInt((TextAnnotation a) -> a.wholeChapter ? -1 : Math.max(0, a.start))
                .thenComparingInt(a -> a.wholeChapter ? -1 : Math.max(0, a.end))
                .thenComparingInt(this::annotationPriority)
                .thenComparing(a -> safe(a.id)));
        return list;
    }

    private int getApproximateYForAnnotation(TextAnnotation annotation) {
        if (annotation == null || readerPane == null) return 0;
        int offset = annotation.wholeChapter ? 0 : sourceOffsetToRenderedOffset(Math.max(0, annotation.start), true);
        try {
            Rectangle r = readerPane.modelToView2D(Math.max(0, Math.min(offset, readerPane.getDocument().getLength()))).getBounds();
            int scrollTop = readerScrollPane == null ? 0 : readerScrollPane.getVerticalScrollBar().getValue();
            return Math.max(0, r.y - scrollTop);
        } catch (Exception ignored) {
            return annotation.wholeChapter ? 0 : Math.max(0, annotation.start / 3);
        }
    }

    private JComponent buildMarginChapterNoteCard(ChapterNote note) {
        Color accent = linkPurple;
        Color cardBg = blend(accent, Color.WHITE, 0.84);
        JPanel card = new JPanel(new BorderLayout(6, 5));
        card.setBackground(cardBg);
        card.setBorder(new CompoundBorder(
                new CompoundBorder(new RoundedBorder(new Color(206, 192, 180), 13, new Insets(1, 1, 1, 1)), new MatteBorder(0, 5, 0, 0, accent)),
                new EmptyBorder(7, 8, 7, 8)));
        card.setMaximumSize(new Dimension(Integer.MAX_VALUE, 170));

        JPanel top = new JPanel(new BorderLayout(6, 2));
        top.setOpaque(false);
        JLabel chip = new JLabel("Chapter Note");
        chip.setFont(new Font("Segoe UI", Font.BOLD, 11));
        chip.setForeground(modernDarkRed);
        chip.setBorder(new CompoundBorder(new RoundedBorder(accent.darker(), 10, new Insets(1, 1, 1, 1)), new EmptyBorder(2, 7, 2, 7)));
        chip.setOpaque(true);
        chip.setBackground(blend(accent, Color.WHITE, 0.55));
        JLabel reference = new JLabel(chapterNoteReference(note));
        reference.setFont(new Font("Segoe UI", Font.BOLD, 13));
        reference.setForeground(modernText);
        top.add(chip, BorderLayout.WEST);
        top.add(reference, BorderLayout.CENTER);
        card.add(top, BorderLayout.NORTH);

        JTextPane body = smartVerseTextPane(note, false);
        body.setText(shortenLines(safe(note.noteText), 260));
        body.setFont(new Font("Segoe UI", Font.PLAIN, 12));
        body.setForeground(modernText);
        body.setBackground(cardBg);
        body.setBorder(new EmptyBorder(0, 0, 0, 0));
        SwingUtilities.invokeLater(() -> applySmartVerseHighlights(body, note));
        card.add(body, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 4, 0));
        actions.setOpaque(false);
        JButton open = smallSidebarActionButton("Open");
        open.addActionListener(e -> openChapterNoteViewer(note));
        JButton edit = smallSidebarActionButton("Edit");
        edit.addActionListener(e -> {
            TextAnnotation annotation = annotationForChapterNote(note);
            if (annotation != null) showChapterAnnotationDialog(annotation, true);
            else showChapterNoteDialog(note, true);
        });
        JButton jump = smallSidebarActionButton("Jump To Chapter");
        jump.addActionListener(e -> jumpToChapterNote(note));
        actions.add(open); actions.add(edit); actions.add(jump);
        card.add(actions, BorderLayout.SOUTH);
        return card;
    }

    private void showChapterAnnotationDialog(TextAnnotation annotation, boolean editMode) {
        if (annotation == null) return;
        repairAnnotation(annotation, System.currentTimeMillis());
        ChapterNote proxy = chapterNoteFromAnnotation(annotation);
        final JDialog dialog = new JDialog(this, "Chapter Notes — " + chapterNoteReference(proxy), false);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.setSize(850, 650);
        dialog.setLocationRelativeTo(this);

        JPanel root = new JPanel(new BorderLayout(10, 10));
        root.setBorder(new EmptyBorder(14, 14, 14, 14));
        root.setBackground(modernBackground);
        JLabel title = new JLabel("Chapter Notes — " + chapterNoteReference(proxy));
        title.setFont(new Font("Segoe UI", Font.BOLD, 22));
        title.setForeground(darkRed);
        root.add(title, BorderLayout.NORTH);

        JTextArea noteArea = new JTextArea(safe(annotation.note));
        noteArea.setEditable(editMode);
        noteArea.setLineWrap(true);
        noteArea.setWrapStyleWord(true);
        noteArea.setFont(new Font("Segoe UI", Font.PLAIN, 16));
        noteArea.setBackground(modernSurface);
        noteArea.setBorder(new EmptyBorder(12, 12, 12, 12));
        JScrollPane scroll = new JScrollPane(noteArea);
        scroll.getVerticalScrollBar().setUnitIncrement(18);
        scroll.setBorder(new RoundedBorder(modernBorder, 12, new Insets(1, 1, 1, 1)));
        root.add(scroll, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        actions.setOpaque(false);
        JButton save = blackButton("Save");
        save.setEnabled(editMode);
        save.addActionListener(e -> {
            annotation.note = noteArea.getText() == null ? "" : noteArea.getText();
            annotation.wholeChapter = true;
            annotation.updatedAt = System.currentTimeMillis();
            saveData();
            refreshRecentNotes();
            refreshMarginNotesPanel();
            dialog.dispose();
        });
        JButton edit = blackButton(editMode ? "Editing" : "Edit");
        edit.addActionListener(e -> { noteArea.setEditable(true); save.setEnabled(true); edit.setText("Editing"); });
        JButton close = blackButton("Close");
        close.addActionListener(e -> dialog.dispose());
        actions.add(edit); actions.add(save); actions.add(close);
        root.add(actions, BorderLayout.SOUTH);
        dialog.setContentPane(root);
        applyModernTheme(dialog);
        dialog.setVisible(true);
    }

    private String shortenLines(String text, int max) {
        return shorten(safe(text).replaceAll("\\s+", " ").trim(), max);
    }

    private JComponent buildMarginNoteCard(TextAnnotation annotation, boolean expanded) {
        Color accent = colorForAnnotation(annotation);
        Color cardBg = blend(accent, Color.WHITE, expanded ? 0.88 : 0.82);
        JPanel card = new JPanel(new BorderLayout(6, 5));
        card.setBackground(cardBg);
        card.setBorder(new CompoundBorder(
                new CompoundBorder(new RoundedBorder(new Color(206, 192, 180), 13, new Insets(1, 1, 1, 1)), new MatteBorder(0, 5, 0, 0, accent)),
                new EmptyBorder(7, 8, 7, 8)));
        card.setMaximumSize(new Dimension(Integer.MAX_VALUE, expanded ? 260 : 150));
        card.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        card.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 1) toggleMarginNoteExpanded(annotation.id);
            }
        });

        JPanel top = new JPanel(new BorderLayout(6, 2));
        top.setOpaque(false);
        JLabel chip = new JLabel(marginNoteChipText(annotation));
        chip.setFont(new Font("Segoe UI", Font.BOLD, 11));
        chip.setForeground(modernDarkRed);
        chip.setBorder(new CompoundBorder(new RoundedBorder(accent.darker(), 10, new Insets(1, 1, 1, 1)), new EmptyBorder(2, 7, 2, 7)));
        chip.setOpaque(true);
        chip.setBackground(blend(accent, Color.WHITE, 0.55));

        JLabel reference = new JLabel(getAnnotationReferenceLabel(annotation));
        reference.setFont(new Font("Segoe UI", Font.BOLD, 13));
        reference.setForeground(modernText);
        top.add(chip, BorderLayout.WEST);
        top.add(reference, BorderLayout.CENTER);
        card.add(top, BorderLayout.NORTH);

        JPanel textStack = new JPanel();
        textStack.setLayout(new BoxLayout(textStack, BoxLayout.Y_AXIS));
        textStack.setOpaque(false);
        String targetPreview = getAnnotationTargetPreview(annotation);
        if (!targetPreview.isEmpty()) {
            JTextArea target = compactMarginText("“" + targetPreview + "”", cardBg, new Font("Segoe UI", Font.ITALIC, 12), modernMutedText);
            textStack.add(target);
            textStack.add(Box.createVerticalStrut(3));
        }
        JTextArea body = compactMarginText(expanded ? marginNoteExpandedText(annotation) : marginNotePreviewText(annotation), cardBg, new Font("Segoe UI", Font.PLAIN, 12), modernText);
        textStack.add(body);
        card.add(textStack, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 4, 0));
        actions.setOpaque(false);
        JButton jump = smallSidebarActionButton("Jump To");
        jump.addActionListener(e -> jumpToMarginNote(annotation));
        JButton edit = smallSidebarActionButton("Edit");
        edit.addActionListener(e -> editAnnotation(annotation));
        JButton delete = smallSidebarActionButton("Delete");
        delete.addActionListener(e -> deleteAnnotation(annotation));
        actions.add(jump);
        actions.add(edit);
        actions.add(delete);
        if (annotationSummaryText(annotation).length() > 150) {
            JButton expand = smallSidebarActionButton(expanded ? "Collapse" : "Expand");
            expand.addActionListener(e -> toggleMarginNoteExpanded(annotation.id));
            actions.add(expand);
        }
        StudyQuestion q = questionForAnnotation(annotation.id);
        if (q != null) {
            JButton addAnswer = smallSidebarActionButton("Answer");
            addAnswer.addActionListener(e -> { promptAddAnswer(q); refreshMarginNotesPanel(); });
            actions.add(addAnswer);
        }
        card.add(actions, BorderLayout.SOUTH);
        return card;
    }

    private JTextArea compactMarginText(String text, Color bg, Font font, Color fg) {
        JTextArea area = readonlyArea();
        area.setText(safe(text));
        area.setFont(font);
        area.setForeground(fg);
        area.setBackground(bg);
        area.setBorder(new EmptyBorder(0, 0, 0, 0));
        area.setLineWrap(true);
        area.setWrapStyleWord(true);
        area.setRows(0);
        area.setMinimumSize(new Dimension(0, 0));
        return area;
    }

    private JButton smallSidebarActionButton(String text) {
        JButton button = new JButton(text);
        button.setFont(new Font("Segoe UI", Font.PLAIN, 11));
        button.setMargin(new Insets(2, 6, 2, 6));
        button.setFocusable(false);
        return button;
    }

    private Color blend(Color a, Color b, double bWeight) {
        double aw = 1.0 - bWeight;
        return new Color(
                (int) Math.max(0, Math.min(255, a.getRed() * aw + b.getRed() * bWeight)),
                (int) Math.max(0, Math.min(255, a.getGreen() * aw + b.getGreen() * bWeight)),
                (int) Math.max(0, Math.min(255, a.getBlue() * aw + b.getBlue() * bWeight)));
    }

    private String marginNoteChipText(TextAnnotation a) {
        String type = safe(a == null ? "" : a.type);
        if ("Question".equals(type)) return questionBubbleLabel(a) + " Question";
        if ("Category".equals(type)) return safe(a.category).isEmpty() ? "Category" : shorten(a.category, 18);
        if ("Link".equals(type)) return "Attachment";
        if (type.isEmpty()) return "Note";
        return type;
    }

    private void toggleMarginNoteExpanded(String annotationId) {
        if (safe(annotationId).isEmpty()) return;
        if (!expandedMarginNoteIds.add(annotationId)) expandedMarginNoteIds.remove(annotationId);
        refreshMarginNotesPanel();
    }

    private void restoreNormalRightSidebar() {
        showRightSidebarMode("search");
        if (centerRightSplit != null) {
            Component right = centerRightSplit.getRightComponent();
            if (right != null) right.setVisible(true);
            centerRightSplit.setDividerSize(7);
            clampCenterRightDivider(true);
        }
        if (marginNotesToggleButton != null) marginNotesToggleButton.setText("Margin Notes");
        revalidate();
        repaint();
    }

    private String getAnnotationReferenceLabel(TextAnnotation a) {
        if (a == null) return fallbackAnnotationSourceTitle(a);
        if (a.wholeChapter) return shortAnnotationChapterReference(a, "Whole Chapter");
        String sourceTitle = fallbackAnnotationSourceTitle(a);
        if (safe(a.sourceKey).startsWith("BIBLE:") || safe(currentSourceKey).startsWith("BIBLE:")) {
            Integer startVerse = verseForAnnotationOffset(a.start);
            Integer endVerse = verseForAnnotationOffset(Math.max(a.start, a.end - 1));
            String chapterTitle = shortBibleReferenceTitle(a, sourceTitle);
            if (chapterTitle.isEmpty()) chapterTitle = shortBibleSourceTitle(currentSourceTitle);
            if (startVerse != null && endVerse != null) {
                return chapterTitle + ":" + (startVerse.equals(endVerse) ? startVerse : startVerse + "–" + endVerse);
            }
            if (startVerse != null) return chapterTitle + ":" + startVerse;
            return chapterTitle.isEmpty() ? "Current Chapter" : chapterTitle;
        }
        return sourceTitle.isEmpty() ? "Current Source" : shorten(sourceTitle, 58);
    }

    private String shortAnnotationChapterReference(TextAnnotation a, String fallback) {
        String ref = shortBibleReferenceTitle(a, fallbackAnnotationSourceTitle(a));
        if (!ref.isEmpty()) return ref;
        return fallback;
    }

    private String shortBibleReferenceTitle(TextAnnotation a, String fallbackTitle) {
        if (a != null && !safe(a.book).isEmpty() && a.chapter > 0) {
            return getShortBookDisplayName(a.book) + " " + a.chapter;
        }
        RefParts rp = refPartsFromBibleSourceKey(a == null ? "" : a.sourceKey);
        if (rp != null) return getShortBookDisplayName(rp.book) + " " + rp.chapter;
        return shortBibleSourceTitle(fallbackTitle);
    }

    private String shortBibleSourceTitle(String title) {
        String raw = safe(title).trim();
        if (raw.startsWith("BIBLE:")) raw = raw.substring("BIBLE:".length()).trim();
        if (raw.isEmpty()) return raw;
        Matcher matcher = Pattern.compile("^(.+?)\\s+(\\d+)(.*)$").matcher(raw);
        if (matcher.matches()) {
            String shortBook = getShortBookDisplayName(matcher.group(1).trim());
            return shortBook + " " + matcher.group(2).trim() + matcher.group(3).trim();
        }
        return getShortBookDisplayName(raw);
    }

    private Integer verseForAnnotationOffset(int sourceOffset) {
        if (readerPane == null) return null;
        Integer verse = verseNumberContainingPosition(sourceOffsetToRenderedOffset(Math.max(0, sourceOffset), true));
        if (verse == null) verse = verseNumberContainingPosition(sourceOffsetToRenderedOffset(Math.max(0, sourceOffset - 1), true));
        return verse;
    }

    private String fallbackAnnotationSourceTitle(TextAnnotation a) {
        String title = a == null ? "" : safe(a.sourceTitle);
        if (title.isEmpty() && a != null) title = safe(a.sourceKey);
        if (title.isEmpty()) title = safe(currentSourceTitle);
        if (title.isEmpty() && !safe(selectedBook).isEmpty()) title = selectedBook + " " + selectedChapter;
        return title;
    }

    private String getAnnotationTargetPreview(TextAnnotation a) {
        if (a == null) return "";
        String text = safe(a.selectedText).trim();
        if (text.isEmpty()) text = safe(a.target).trim();
        text = text.replaceAll("\\s+", " ");
        return shorten(text, 90);
    }

    private String marginNoteLocation(TextAnnotation a) {
        return getAnnotationReferenceLabel(a);
    }

    private String marginNotePreviewText(TextAnnotation a) {
        String text = annotationSummaryText(a).replace("\n", " ");
        if ("Question".equals(safe(a.type))) {
            text = questionTypeDisplay(questionTypeForAnnotation(a)) + " • " + text;
        } else if (!safe(a.category).isEmpty() && !"Category".equals(safe(a.type))) {
            text = "Category: " + safe(a.category) + " • " + text;
        }
        return shorten(text, 165);
    }

    private String marginNoteExpandedText(TextAnnotation a) {
        StringBuilder sb = new StringBuilder();
        if ("Question".equals(safe(a.type))) {
            StudyQuestion q = questionForAnnotation(a.id);
            sb.append(questionTypeDisplay(q == null ? questionTypeForAnnotation(a) : q.questionType)).append(" — ");
            sb.append(q != null && q.answered ? "Answered" : "Unanswered");
            sb.append(q == null ? "" : " • " + q.answers.size() + " answer(s)").append("\n\n");
        } else if (!safe(a.category).isEmpty()) {
            sb.append("Category: ").append(a.category).append("\n\n");
        }
        sb.append(annotationSummaryText(a));
        StudyQuestion q = questionForAnnotation(a.id);
        if (q != null && !q.answers.isEmpty()) sb.append("\n\nAnswers:\n").append(answersSummary(q));
        return sb.toString();
    }

    private void jumpToMarginNote(TextAnnotation a) {
        if (a == null) return;
        openSourceForAnnotation(a);
        if (a.wholeChapter) moveReaderCaret(0); else safeSelect(a.start, a.end);
        showAnnotationDetails(a);
        showCard("study");
        showRightSidebarMode("margin");
        SwingUtilities.invokeLater(this::refreshMarginNotesPanel);
    }

    private int currentReaderBodyFontSize() {
        return readingMode ? normalReaderFontSize + 3 : normalReaderFontSize;
    }

    private void setReaderBodyFontSize(int size) {
        if (readerPane == null) return;
        readerPane.setFont(readerPane.getFont().deriveFont((float) size));
        try {
            StyledDocument doc = readerPane.getStyledDocument();
            SimpleAttributeSet attrs = new SimpleAttributeSet();
            StyleConstants.setFontSize(attrs, size);
            doc.setCharacterAttributes(0, doc.getLength(), attrs, false);
            applyBaseHeadingStyle();
        } catch (Exception ignored) {}
    }

    private void showBlankReader() {
        loadingReader = true;
        try {
            currentSourceKey = "";
            currentSourceTitle = "";
            readerPane.setText("No Bible text loaded yet. Click Import, then download/import BSB USFM or import a Bible CSV.\n\nYou can also import TXT files for philosophy works and annotate selected phrases inside them.");
            sourceLabel.setText("No source loaded");
            showDetailsText("Select text and right-click to add a note, category, question, or attachment.");
        } finally {
            loadingReader = false;
        }
    }

    private void showSelectedChapter(boolean countVisit) {
        if (readerPane == null || selectedBook == null || selectedBook.isEmpty() || !data.bible.containsKey(selectedBook)) {
            showBlankReader();
            return;
        }

        currentSourceKey = "BIBLE:" + selectedBook + " " + selectedChapter;
        currentSourceTitle = selectedBook + " " + selectedChapter;

        if (countVisit) {
            currentProfile.visitCounts.put(currentSourceKey, currentProfile.visitCounts.getOrDefault(currentSourceKey, 0) + 1);
            saveData();
        }

        StringBuilder sb = new StringBuilder();
        sb.append(selectedBook).append(" ").append(selectedChapter).append("\n\n");
        for (Verse v : data.getVerses(selectedBook, selectedChapter).values()) {
            sb.append(v.verse).append(" ").append(v.text).append("\n\n");
        }

        setReaderText(sb.toString(), currentSourceKey, currentSourceTitle);
        int visits = currentProfile.visitCounts.getOrDefault(currentSourceKey, 0);
        sourceLabel.setText(currentSourceTitle + " — visited " + visits + " time" + (visits == 1 ? "" : "s"));
    }

    private void showLibraryDoc(String title) {
        LibraryDoc d = data.findLibraryDoc(title);
        if (d == null) return;
        currentSourceKey = "LIBRARY:" + d.title;
        currentSourceTitle = d.title;
        setReaderText(d.title + "\n\n" + d.body, currentSourceKey, currentSourceTitle);
        sourceLabel.setText("Library: " + d.title);
    }

    private void setReaderText(String text, String sourceKey, String sourceTitle) {
        loadingReader = true;
        try {
            StyledDocument doc = readerPane.getStyledDocument();
            doc.remove(0, doc.getLength());
            Style normal = style(doc, "normal", "Georgia", currentReaderBodyFontSize(), false, new Color(45, 35, 30), null);
            closeAllAnnotationBubblePopups();
            visibleAnnotationBubbles.clear();
            doc.insertString(0, text, normal);
            applyBaseHeadingStyle();
            renderAnnotationBubbles(sourceKey);
            applyAnnotationsForSource(sourceKey);
            readerPane.setCaretPosition(0);
            showSourceSummary(sourceKey, sourceTitle);
            if (marginNotesMode) SwingUtilities.invokeLater(this::refreshMarginNotesPanel);
            updateHeader();
            SwingUtilities.invokeLater(this::trackReaderLocation);
        } catch (Exception ex) {
            showError("Reader display failed", ex);
        } finally {
            loadingReader = false;
        }
    }

    private Style style(StyledDocument doc, String name, String family, int size, boolean bold, Color fg, Color bg) {
        Style st = doc.getStyle(name);
        if (st == null) st = doc.addStyle(name, null);
        StyleConstants.setFontFamily(st, family);
        StyleConstants.setFontSize(st, size);
        StyleConstants.setBold(st, bold);
        StyleConstants.setForeground(st, fg);
        if (bg != null) StyleConstants.setBackground(st, bg);
        return st;
    }

    private void applyBaseHeadingStyle() throws BadLocationException {
        StyledDocument doc = readerPane.getStyledDocument();
        String txt = doc.getText(0, doc.getLength());
        int firstBreak = txt.indexOf('\n');
        if (firstBreak >= 0) {
            SimpleAttributeSet heading = new SimpleAttributeSet();
            StyleConstants.setFontFamily(heading, "Segoe UI");
            StyleConstants.setFontSize(heading, 26);
            StyleConstants.setBold(heading, true);
            StyleConstants.setForeground(heading, darkRed);
            doc.setCharacterAttributes(0, firstBreak, heading, false);
        }
    }

    private void applyAnnotationsForSource(String sourceKey) {
        StyledDocument doc = readerPane.getStyledDocument();
        readerPane.getHighlighter().removeAllHighlights();
        int originalLen = readerOriginalLength();
        Set<String> paintedRanges = new HashSet<>();
        java.util.List<TextAnnotation> annotations = new ArrayList<>();
        for (TextAnnotation a : currentProfile.annotations) {
            if (!sourceKey.equals(a.sourceKey) || a.wholeChapter) continue;
            if (a.start < 0 || a.end <= a.start || a.start >= originalLen) continue;
            annotations.add(a);
        }
        annotations.sort(Comparator
                .comparingInt((TextAnnotation a) -> Math.max(0, a.end - a.start)).reversed()
                .thenComparingInt(this::annotationPriority)
                .thenComparing(a -> safe(a.id)));

        for (TextAnnotation a : annotations) {
            int safeEnd = Math.min(a.end, originalLen);
            String rangeKey = annotationRangeKey(sourceKey, a.start, safeEnd);
            if (!paintedRanges.add(rangeKey)) continue;

            java.util.List<TextAnnotation> rangeAnnotations = getAnnotationsForRange(sourceKey, a.start, safeEnd);
            TextAnnotation primary = primaryAnnotation(rangeAnnotations);
            TextAnnotation secondary = secondaryAnnotation(rangeAnnotations, primary);
            int renderedStart = sourceOffsetToRenderedOffset(a.start, true);
            int renderedEnd = sourceOffsetToRenderedOffset(safeEnd, false);
            if (renderedEnd <= renderedStart) continue;

            SimpleAttributeSet set = new SimpleAttributeSet();
            StyleConstants.setBackground(set, colorForAnnotation(primary == null ? a : primary));
            StyleConstants.setForeground(set, new Color(35, 25, 20));
            StyleConstants.setBold(set, anyAnnotationType(rangeAnnotations, "Question"));
            doc.setCharacterAttributes(renderedStart, renderedEnd - renderedStart, set, false);

            if (rangeAnnotations.size() > 1 && secondary != null) {
                try {
                    readerPane.getHighlighter().addHighlight(renderedStart, renderedEnd,
                            new AnnotationUnderlinePainter(colorForAnnotation(secondary)));
                } catch (BadLocationException ignored) {}
            }
        }
    }

    private void renderAnnotationBubbles(String sourceKey) throws BadLocationException {
        visibleAnnotationBubbles.clear();
        if (currentProfile == null || sourceKey == null) return;
        StyledDocument doc = readerPane.getStyledDocument();
        int originalLen = doc.getLength();
        Map<String, java.util.List<TextAnnotation>> grouped = new LinkedHashMap<>();
        for (TextAnnotation a : currentProfile.annotations) {
            if (!sourceKey.equals(a.sourceKey) || !hasReaderBubble(a)) continue;
            if (a.wholeChapter) {
                int headingEnd = chapterHeadingEndOffset();
                grouped.computeIfAbsent(annotationRangeKey(sourceKey, 0, headingEnd) + "|chapter", k -> getWholeChapterAnnotations(sourceKey));
                continue;
            }
            if (a.start < 0 || a.end <= a.start || a.start >= originalLen) continue;
            int safeEnd = Math.min(a.end, originalLen);
            String rangeKey = annotationRangeKey(sourceKey, a.start, safeEnd);
            grouped.computeIfAbsent(rangeKey, k -> getAnnotationsForRange(sourceKey, a.start, safeEnd));
        }
        java.util.List<java.util.List<TextAnnotation>> bubbleGroups = new ArrayList<>(grouped.values());
        bubbleGroups.sort(Comparator
                .comparingInt((java.util.List<TextAnnotation> group) -> group.stream().mapToInt(x -> Math.min(x.end, originalLen)).max().orElse(0))
                .thenComparing(group -> safe(primaryAnnotation(group).type))
                .thenComparing(group -> safe(primaryAnnotation(group).id)));

        int inserted = 0;
        for (java.util.List<TextAnnotation> group : bubbleGroups) {
            if (group.isEmpty()) continue;
            TextAnnotation primary = primaryAnnotation(group);
            int sourceOffset = primary.wholeChapter ? chapterHeadingEndOffset() : Math.max(0, Math.min(primary.end, originalLen));
            int renderedOffset = sourceOffset + inserted;
            String markerText = bubbleTextForAnnotations(group);
            SimpleAttributeSet bubbleStyle = bubbleStyleForAnnotations(group);
            doc.insertString(renderedOffset, markerText, bubbleStyle);
            visibleAnnotationBubbles.add(new AnnotationBubbleMarker(primary, group, markerText, sourceOffset, renderedOffset, renderedOffset + markerText.length()));
            inserted += markerText.length();
        }
    }

    private boolean hasReaderBubble(TextAnnotation a) {
        return a != null && ("Note".equals(a.type) || "Question".equals(a.type));
    }

    private String bubbleTextForAnnotation(TextAnnotation a) {
        return bubbleTextForAnnotations(Collections.singletonList(a));
    }

    private String bubbleTextForAnnotations(java.util.List<TextAnnotation> annotations) {
        int count = annotations == null ? 0 : annotations.size();
        if (count > 1) return "  💬" + count + " ";
        TextAnnotation a = annotations == null || annotations.isEmpty() ? null : annotations.get(0);
        if ("Question".equals(a == null ? "" : a.type)) return "  " + questionBubbleLabel(a) + " ";
        return "  📝 ";
    }

    private SimpleAttributeSet bubbleStyleForAnnotation(TextAnnotation a) {
        return bubbleStyleForAnnotations(Collections.singletonList(a));
    }

    private SimpleAttributeSet bubbleStyleForAnnotations(java.util.List<TextAnnotation> annotations) {
        TextAnnotation primary = primaryAnnotation(annotations);
        boolean multi = annotations != null && annotations.size() > 1;
        SimpleAttributeSet set = new SimpleAttributeSet();
        StyleConstants.setFontFamily(set, "Segoe UI Emoji");
        StyleConstants.setFontSize(set, Math.max(12, currentReaderBodyFontSize() - 1));
        StyleConstants.setBold(set, true);
        StyleConstants.setForeground(set, multi ? new Color(78, 46, 118) : ("Question".equals(safe(primary == null ? "" : primary.type)) ? new Color(44, 72, 135) : new Color(82, 88, 27)));
        StyleConstants.setBackground(set, multi ? new Color(240, 230, 255) : bubbleBackgroundForAnnotation(primary));
        StyleConstants.setUnderline(set, true);
        return set;
    }


    private int chapterHeadingEndOffset() {
        try {
            String txt = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            int firstBreak = txt.indexOf('\n');
            return firstBreak >= 0 ? firstBreak : txt.length();
        } catch (Exception ignored) {
            return 0;
        }
    }

    private boolean isChapterTitleOffset(int renderedOffset) {
        return currentSourceKey != null && currentSourceKey.startsWith("BIBLE:") && renderedOffset >= 0 && renderedOffset <= chapterHeadingEndOffset();
    }

    private java.util.List<TextAnnotation> getWholeChapterAnnotations(String sourceKey) {
        java.util.List<TextAnnotation> list = new ArrayList<>();
        if (currentProfile == null || sourceKey == null) return list;
        for (TextAnnotation a : currentProfile.annotations) {
            if (a != null && a.wholeChapter && sourceKey.equals(a.sourceKey) && hasReaderBubble(a)) list.add(a);
        }
        list.sort(Comparator.comparingInt((TextAnnotation a) -> annotationPriority(a)).thenComparing(a -> safe(a.id)));
        return list;
    }

    private Color questionColorForType(String type) {
        return "personal".equals(normalizeQuestionType(type)) ? personalQuestionOrange : discussionQuestionBlue;
    }

    private Color bubbleBackgroundForAnnotation(TextAnnotation a) {
        if (a != null && "Question".equals(a.type)) return questionColorForType(questionTypeForAnnotation(a));
        return new Color(255, 250, 190);
    }

    private String questionBubbleLabel(TextAnnotation a) {
        return "personal".equals(questionTypeForAnnotation(a)) ? "PQ" : "DQ";
    }

    private String questionTypeForAnnotation(TextAnnotation a) {
        if (a == null) return "discussion";
        String cat = normalizeQuestionType(a.category);
        if (!cat.isEmpty()) return cat;
        StudyQuestion q = questionForAnnotation(a.id);
        return q == null ? "discussion" : normalizeQuestionType(q.questionType);
    }

    private String normalizeQuestionType(String type) {
        String t = safe(type).trim().toLowerCase(Locale.ROOT);
        if (t.startsWith("p")) return "personal";
        return "discussion";
    }

    private String questionTypeDisplay(String type) {
        return "personal".equals(normalizeQuestionType(type)) ? "Personal Question" : "Discussion Question";
    }

    private int readerOriginalLength() {
        int len = readerPane == null ? 0 : readerPane.getDocument().getLength();
        for (AnnotationBubbleMarker marker : visibleAnnotationBubbles) len -= Math.max(0, marker.endOffset - marker.startOffset);
        return Math.max(0, len);
    }

    private int sourceOffsetToRenderedOffset(int sourceOffset, boolean includeInsertionsAtOffset) {
        int rendered = Math.max(0, sourceOffset);
        for (AnnotationBubbleMarker marker : visibleAnnotationBubbles) {
            if (marker.sourceOffset < sourceOffset || (includeInsertionsAtOffset && marker.sourceOffset == sourceOffset)) {
                rendered += Math.max(0, marker.endOffset - marker.startOffset);
            }
        }
        int len = readerPane == null ? rendered : readerPane.getDocument().getLength();
        return Math.max(0, Math.min(rendered, len));
    }

    private int renderedOffsetToSourceOffset(int renderedOffset) {
        int source = Math.max(0, renderedOffset);
        for (AnnotationBubbleMarker marker : visibleAnnotationBubbles) {
            int markerLen = Math.max(0, marker.endOffset - marker.startOffset);
            if (renderedOffset >= marker.endOffset) source -= markerLen;
            else if (renderedOffset >= marker.startOffset) return marker.sourceOffset;
        }
        return Math.max(0, Math.min(source, readerOriginalLength()));
    }

    private AnnotationBubbleMarker bubbleAt(int pos) {
        for (AnnotationBubbleMarker marker : visibleAnnotationBubbles) {
            if (pos >= marker.startOffset && pos < marker.endOffset) return marker;
        }
        return null;
    }

    private Color colorForAnnotation(TextAnnotation a) {
        String type = a == null ? "" : safe(a.type);
        if ("Category".equals(type)) return colorForCategory(a.category);
        if ("Link".equals(type) || "Topic".equals(type)) return linkPurple;
        if ("Question".equals(type)) return questionColorForType(questionTypeForAnnotation(a));
        if ("Greek".equals(type)) return greekGreen;
        if ("Memory".equals(type) || "Memory Verse".equals(type)) return new Color(245, 190, 55);
        if ("Attachment".equals(type)) return new Color(145, 166, 190);
        return noteYellow;
    }

    private Color colorForCategory(String category) {
        if (category == null || category.trim().isEmpty()) return categoryBlue;
        if (currentProfile != null && currentProfile.categoryColors != null) {
            Integer rgb = currentProfile.categoryColors.get(category);
            if (rgb != null) return new Color(rgb, true);
        }
        return categoryBlue;
    }

    private String annotationRangeKey(String sourceKey, int start, int end) {
        return safe(sourceKey) + "|" + Math.max(0, start) + "|" + Math.max(0, end);
    }

    private java.util.List<TextAnnotation> getAnnotationsAtOffset(int renderedOffset) {
        if (currentProfile == null || currentSourceKey == null) return new ArrayList<>();
        int sourcePos = renderedOffsetToSourceOffset(renderedOffset);
        java.util.List<TextAnnotation> matches = new ArrayList<>();
        for (TextAnnotation a : currentProfile.annotations) {
            if (a != null && !a.wholeChapter && currentSourceKey.equals(a.sourceKey) && sourcePos >= a.start && sourcePos < a.end) matches.add(a);
        }
        matches.sort(this::compareAnnotationsForDisplay);
        return matches;
    }

    private java.util.List<TextAnnotation> getAnnotationsForRange(String sourceKey, int start, int end) {
        java.util.List<TextAnnotation> matches = new ArrayList<>();
        if (currentProfile == null || sourceKey == null) return matches;
        for (TextAnnotation a : currentProfile.annotations) {
            if (a != null && !a.wholeChapter && sourceKey.equals(a.sourceKey) && a.start == start && a.end == end) matches.add(a);
        }
        matches.sort(this::compareAnnotationsForDisplay);
        return matches;
    }

    private java.util.List<TextAnnotation> getOverlappingAnnotations(TextAnnotation annotation) {
        java.util.List<TextAnnotation> matches = new ArrayList<>();
        if (annotation == null || currentProfile == null) return matches;
        for (TextAnnotation a : currentProfile.annotations) {
            if (a != null && safe(annotation.sourceKey).equals(safe(a.sourceKey)) && rangesOverlap(annotation.start, annotation.end, a.start, a.end)) matches.add(a);
        }
        matches.sort(this::compareAnnotationsForDisplay);
        return matches;
    }

    private boolean rangesOverlap(int aStart, int aEnd, int bStart, int bEnd) {
        return Math.max(aStart, bStart) < Math.min(aEnd, bEnd);
    }

    private int compareAnnotationsForDisplay(TextAnnotation a, TextAnnotation b) {
        int p = Integer.compare(annotationPriority(a), annotationPriority(b));
        if (p != 0) return p;
        int c = Long.compare(a == null ? 0 : a.createdAt, b == null ? 0 : b.createdAt);
        if (c != 0) return c;
        return safe(a == null ? "" : a.id).compareTo(safe(b == null ? "" : b.id));
    }

    private int annotationPriority(TextAnnotation a) {
        return annotationPriority(a == null ? "" : a.type);
    }

    private int annotationPriority(String type) {
        if ("Question".equals(type)) return 1;
        if ("Note".equals(type)) return 2;
        if ("Category".equals(type)) return 3;
        if ("Link".equals(type) || "Topic".equals(type)) return 4;
        if ("Greek".equals(type)) return 5;
        if ("Memory".equals(type) || "Memory Verse".equals(type)) return 6;
        if ("Attachment".equals(type)) return 7;
        return 99;
    }

    private TextAnnotation primaryAnnotation(java.util.List<TextAnnotation> annotations) {
        if (annotations == null || annotations.isEmpty()) return null;
        for (TextAnnotation a : annotations) if ("Category".equals(a.type)) return a;
        return annotations.stream().min(this::compareAnnotationsForDisplay).orElse(annotations.get(0));
    }

    private TextAnnotation secondaryAnnotation(java.util.List<TextAnnotation> annotations, TextAnnotation primary) {
        if (annotations == null) return null;
        return annotations.stream()
                .filter(a -> a != null && (primary == null || !safe(a.id).equals(safe(primary.id))))
                .min(this::compareAnnotationsForDisplay)
                .orElse(null);
    }

    private boolean anyAnnotationType(java.util.List<TextAnnotation> annotations, String type) {
        if (annotations == null) return false;
        for (TextAnnotation a : annotations) if (a != null && type.equals(a.type)) return true;
        return false;
    }

    private String colorHex(Color c) {
        if (c == null) c = categoryBlue;
        return String.format("#%02X%02X%02X", c.getRed(), c.getGreen(), c.getBlue());
    }


    private void hideSelectionActionPopup() {
        if (selectionActionPopup != null && selectionActionPopup.isVisible()) {
            selectionActionPopup.setVisible(false);
        }
    }

    private void showSelectionActionPopupIfNeeded() {
        if (loadingReader || currentSourceKey == null || currentSourceKey.isEmpty()) return;
        if (readerPane.getSelectionEnd() <= readerPane.getSelectionStart()) {
            hideSelectionActionPopup();
            return;
        }
        String selected = readerPane.getSelectedText();
        if (selected == null || selected.trim().isEmpty()) {
            hideSelectionActionPopup();
            return;
        }

        hideSelectionActionPopup();
        selectionActionPopup = new JPopupMenu();
        addSelectedTextActions(selectionActionPopup);

        Point popupPoint = selectionPopupPoint();
        selectionActionPopup.show(readerPane, popupPoint.x, popupPoint.y);
    }


    private void addSelectedTextActions(JPopupMenu menu) {
        boolean bibleSelection = greekKeyForSelection() != null;
        addMenu(menu, "Add Note", () -> addAnnotationFromSelection("Note", ""));
        addMenu(menu, "Add To Category", this::addCategoryFromSelection);
        addMenu(menu, "Add Discussion Question", () -> addAnnotationFromSelection("Question", "discussion"));
        addMenu(menu, "Add Personal Question", () -> addAnnotationFromSelection("Question", "personal"));
        addMenu(menu, "Attach To Bible Verse Or Book Section", this::addAttachmentFromSelection);
        if (bibleSelection) {
            addMenu(menu, "View Greek For This Verse", this::showGreekForCurrentSelection);
            addMenu(menu, "Add Greek Note To Selected Phrase", this::addGreekNoteForSelectionOrVerse);
            addMenu(menu, "Search This In Greek", this::searchSelectedTextInGreek);
            addMenu(menu, "Add Greek Entry to Topic Page", this::addGreekSelectionToTopicPage);
        } else if (currentSourceKey != null && currentSourceKey.startsWith("BIBLE:")) {
            addMenu(menu, "Search This In Greek", this::searchSelectedTextInGreek);
        }
        addMenu(menu, "Pin Selected Text To Sidebar", this::pinSelectedTextToSidebar);
        addMenu(menu, "Add Selected Text To Study Project", this::addSelectedTextToStudyProject);
        addMenu(menu, "Add Selected Text to Topic Page", this::addSelectedTextToTopicPage);
        addMenu(menu, "Create Topic From This", this::createTopicFromCurrentSelection);
        if (currentSourceKey != null && currentSourceKey.startsWith("BIBLE:")) {
            addMenu(menu, "Add To Memory Verses", this::addMemoryVerseFromSelection);
        }
        addMenu(menu, "Add Bookmark Here", () -> addBookmarkAtPosition(readerPane.getSelectionStart(), true, ""));
    }

    private void addExistingHighlightActions(JPopupMenu menu, TextAnnotation existing) {
        addMenu(menu, "View Highlight Details", () -> showAnnotationDetails(existing));
        java.util.List<TextAnnotation> attached = getAnnotationsForRange(existing.sourceKey, existing.start, existing.end);
        if (attached.size() > 1) addMenu(menu, "View All Attached Annotations (" + attached.size() + ")", () -> showAnnotationDetails(attached));
        addMenu(menu, "Edit This Highlight", () -> editAnnotation(existing));
        addMenu(menu, "Pin This Highlight To Sidebar", () -> pinAnnotationToSidebar(existing));
        addMenu(menu, "Add This Note To Study Project", () -> addAnnotationToStudyProject(existing));
        addMenu(menu, "Add Selected Note to Topic Page", () -> addAnnotationToTopicPage(existing));
        if ("Category".equals(existing.type) && existing.category != null && !existing.category.trim().isEmpty()) {
            String category = existing.category.trim();
            addMenu(menu, "Show Category: " + category, () -> showCategoryByName(category));
            addMenu(menu, "Change Category", () -> changeAnnotationCategory(existing));
            addMenu(menu, "Change Category Color", () -> {
                changeCategoryColorByName(category);
                showAnnotationDetails(existing);
            });
            addMenu(menu, "Remove From Category", () -> removeAnnotationFromCategory(existing));
        }
        if (existing.target != null && !existing.target.trim().isEmpty()) {
            addMenu(menu, "Open Attachment", () -> openAnnotationTarget(existing));
        }
        addMenu(menu, "Delete This Highlight", () -> deleteAnnotation(existing));
    }

    private Point selectionPopupPoint() {
        try {
            int anchor = Math.min(readerPane.getSelectionStart(), readerPane.getSelectionEnd());
            Rectangle r = readerPane.modelToView2D(anchor).getBounds();
            int x = Math.max(0, r.x);
            int y = Math.max(0, r.y - 34);
            return new Point(x, y);
        } catch (Exception ignored) {
            return new Point(12, 12);
        }
    }


    private void showChapterTitleMenu(int x, int y) {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return;
        JPopupMenu menu = new JPopupMenu();
        addMenu(menu, "Add Whole Chapter to Category", this::addWholeChapterToCategory);
        addMenu(menu, "Add Whole Chapter Note", this::addWholeChapterNote);
        addMenu(menu, "Add Discussion Question", () -> addWholeChapterQuestion("discussion"));
        addMenu(menu, "Add Personal Question", () -> addWholeChapterQuestion("personal"));
        addMenu(menu, "View Chapter Notes / Questions", this::viewWholeChapterAnnotations);
        menu.show(readerPane, x, y);
    }

    private void addWholeChapterToCategory() {
        String cat = chooseOrCreateCategory();
        if (cat == null || cat.trim().isEmpty()) return;
        TextAnnotation a = chapterAnnotation("Category", cat.trim(), "Whole chapter category: " + cat.trim());
        currentProfile.annotations.add(a);
        saveData();
        refreshCategories();
        refreshRecentNotes();
        reloadCurrentSource();
        showCategoryByName(cat.trim());
    }

    private void addWholeChapterNote() {
        String note = promptMultiline("Add Whole Chapter Note", "Note for " + currentSourceTitle + ":", "");
        if (note == null || note.trim().isEmpty()) return;
        TextAnnotation a = chapterAnnotation("Note", "", note.trim());
        currentProfile.annotations.add(a);
        saveData();
        refreshRecentNotes();
        refreshMarginNotesPanel();
        refreshCategories();
        refreshStudyProjects();
        refreshTopicPages();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void addWholeChapterQuestion(String questionType) {
        String body = promptMultiline("Add " + questionTypeDisplay(questionType), "Question for " + currentSourceTitle + ":", "");
        if (body == null || body.trim().isEmpty()) return;
        TextAnnotation a = chapterAnnotation("Question", normalizeQuestionType(questionType), body.trim());
        currentProfile.annotations.add(a);
        currentProfile.questions.add(new StudyQuestion(a.id, currentSourceTitle, currentSourceTitle, body.trim(), normalizeQuestionType(questionType), currentSourceKey, currentSourceTitle, selectedBook, selectedChapter, true));
        saveData();
        refreshQuestions();
        refreshRecentNotes();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private TextAnnotation chapterAnnotation(String type, String category, String note) {
        int end = Math.max(1, chapterHeadingEndOffset());
        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, 0, end, currentSourceTitle, type, category, note, "");
        a.book = selectedBook;
        a.chapter = selectedChapter;
        a.wholeChapter = true;
        return a;
    }

    private void viewWholeChapterAnnotations() {
        java.util.List<TextAnnotation> list = getWholeChapterAnnotations(currentSourceKey);
        if (list.isEmpty()) {
            JOptionPane.showMessageDialog(this, "No chapter-level notes or questions yet for " + currentSourceTitle + ".");
            return;
        }
        showAnnotationDetails(list);
    }

    private String promptMultiline(String title, String label, String initial) {
        JTextArea area = new JTextArea(safe(initial), 8, 44);
        area.setLineWrap(true);
        area.setWrapStyleWord(true);
        JPanel panel = new JPanel(new BorderLayout(6, 6));
        panel.add(new JLabel(label), BorderLayout.NORTH);
        panel.add(new JScrollPane(area), BorderLayout.CENTER);
        int r = JOptionPane.showConfirmDialog(this, panel, title, JOptionPane.OK_CANCEL_OPTION);
        return r == JOptionPane.OK_OPTION ? area.getText() : null;
    }

    private void showReaderMenu(MouseEvent e) {
        if (loadingReader || currentSourceKey == null || currentSourceKey.isEmpty()) return;

        int pos = readerPane.viewToModel2D(e.getPoint());
        if (isChapterTitleOffset(pos)) {
            showChapterTitleMenu(e.getX(), e.getY());
            return;
        }
        TextAnnotation existing = annotationAt(pos);
        boolean hasSelection = readerPane.getSelectionEnd() > readerPane.getSelectionStart();

        if (!hasSelection && existing == null) selectWordAt(pos);
        hasSelection = readerPane.getSelectionEnd() > readerPane.getSelectionStart();

        Integer verseNumber = verseNumberAtPosition(pos);
        JPopupMenu menu = new JPopupMenu();
        if (!hasSelection) {
            addMenu(menu, "Add Bookmark Here", () -> addBookmarkAtPosition(pos, true, ""));
            menu.addSeparator();
        }

        Integer containingVerseNumber = currentSourceKey.startsWith("BIBLE:") ? verseNumberContainingPosition(pos) : null;
        if (!hasSelection && containingVerseNumber != null) {
            String key = selectedBook + " " + selectedChapter + ":" + containingVerseNumber;
            addMenu(menu, "Add Verse To Memory", () -> addMemoryVerseByKey(key));
            addMenu(menu, "Add Current Verse to Topic Page", () -> addLinkedItemToTopicPage(new LinkedItem("VERSE", key, "related")));
            addMenu(menu, "Create Topic From This", () -> createTopicFromLinkedItem(new LinkedItem("VERSE", key, "related")));
        }

        if (verseNumber != null) {
            String key = selectedBook + " " + selectedChapter + ":" + verseNumber;
            addMenu(menu, "Check Greek For This Verse", () -> showGreekForVerse(key));
            addMenu(menu, "Add Greek Note To This Verse", () -> showGreekForVerse(key));
            menu.addSeparator();
        } else if (containingVerseNumber != null) {
            menu.addSeparator();
        }

        if (hasSelection) {
            addSelectedTextActions(menu);
            menu.addSeparator();
        }

        if (existing != null) {
            addExistingHighlightActions(menu, existing);
        }

        if (menu.getComponentCount() == 0) addMenu(menu, "Select text first", () -> {});
        menu.show(readerPane, e.getX(), e.getY());
    }

    private void addBookmarkFromCurrentCaret(boolean fromBookmarkButton) {
    if (readerPane == null || currentSourceKey == null || currentSourceKey.isEmpty()) return;

    // The toolbar bookmark button should follow the visible scroll position,
    // not the old caret position. This is especially important for imported TXT books.
    int visiblePosition = getTopVisibleDocumentPosition();
    int viewportY = getCurrentReaderViewportY();

    if (fromBookmarkButton) {
        saveOrMoveReadingSpotBookmark(visiblePosition, viewportY);
        return;
    }

    addBookmarkAtPosition(visiblePosition, true, "General");
}
private int getCurrentReaderViewportY() {
    try {
        JViewport viewport = (JViewport) SwingUtilities.getAncestorOfClass(JViewport.class, readerPane);
        if (viewport != null) return Math.max(0, viewport.getViewPosition().y);
    } catch (Exception ignored) {}
    return -1;
}

private int getTopVisibleDocumentPosition() {
    try {
        Rectangle visible = readerPane.getVisibleRect();

        // A tiny inset keeps the point inside real visible text instead of the border/margin.
        Point topVisiblePoint = new Point(
                Math.max(0, visible.x + 6),
                Math.max(0, visible.y + 6)
        );

        int pos = readerPane.viewToModel2D(topVisiblePoint);
        int len = readerPane.getDocument().getLength();

        if (pos >= 0) return Math.max(0, Math.min(pos, len));
    } catch (Exception ignored) {}

    // Fallback only if Swing cannot convert the visible point.
    int len = readerPane.getDocument().getLength();
    return Math.max(0, Math.min(readerPane.getCaretPosition(), len));
}

private void saveOrMoveReadingSpotBookmark(int position, int viewportY) {
    if (currentProfile == null || readerPane == null || currentSourceKey == null || currentSourceKey.isEmpty()) return;
    if (currentProfile.bookmarks == null) currentProfile.bookmarks = new ArrayList<>();

    int len = readerPane.getDocument().getLength();
    int safePosition = Math.max(0, Math.min(position, len));

    String type = currentSourceKey.startsWith("BIBLE:") ? "BibleOverall" :
            currentSourceKey.startsWith("LIBRARY:") ? "LibraryReadingSpot" :
                    "ReadingSpot";

    StudyBookmark bookmark = null;

    String currentRowKey = bookmarkRowKeyForCurrentLocation();
    // This makes the toolbar bookmark "move" the bookmark for that book/source/chapter
    // instead of creating a new duplicate every time.
    for (StudyBookmark b : currentProfile.bookmarks) {
        if (b != null && currentRowKey.equals(bookmarkRowKey(b))) {
            bookmark = b;
            break;
        }
    }

    if (bookmark == null) {
        bookmark = new StudyBookmark();
        bookmark.id = UUID.randomUUID().toString();
        bookmark.createdAt = System.currentTimeMillis();
        currentProfile.bookmarks.add(bookmark);
    }

    String preview = previewAround(safePosition);

    bookmark.title = currentSourceKey.startsWith("BIBLE:")
            ? "Last Bible Reading Spot"
            : "Reading Spot — " + safe(currentSourceTitle);

    bookmark.sourceKey = currentSourceKey;
    bookmark.sourceTitle = currentSourceTitle;
    bookmark.caretPosition = safePosition;
    bookmark.selectionStart = -1;
    bookmark.selectionEnd = -1;
    bookmark.previewText = preview;
    bookmark.type = type;
    bookmark.viewportY = viewportY;
    bookmark.hasViewportY = viewportY >= 0;
    bookmark.updatedAt = System.currentTimeMillis();
    saveSingleBookmarkForCurrentLocation(bookmark);

    saveData();
    refreshLibraryTree();
    updateHeader();
    log("Bookmark moved to current reading spot: " + bookmark.title);
}
    private void addBookmarkAtPosition(int position, boolean askForTitle, String typeOverride) {
        if (currentProfile == null || readerPane == null || currentSourceKey == null || currentSourceKey.isEmpty()) return;
        if (currentProfile.bookmarks == null) currentProfile.bookmarks = new ArrayList<>();
        int len = readerPane.getDocument().getLength();
        int caret = Math.max(0, Math.min(position, len));
        int selectionStart = readerPane.getSelectionStart();
        int selectionEnd = readerPane.getSelectionEnd();
        if (selectionEnd <= selectionStart || caret < selectionStart || caret > selectionEnd) {
            selectionStart = -1;
            selectionEnd = -1;
        }
        String preview = previewAround(caret);
        String defaultTitle = defaultBookmarkTitle(currentSourceTitle, preview);
        String title = defaultTitle;
        if (askForTitle) {
            String entered = JOptionPane.showInputDialog(this, "Bookmark title (optional):", defaultTitle);
            if (entered == null) return;
            if (!entered.trim().isEmpty()) title = entered.trim();
        }

        StudyBookmark bookmark = new StudyBookmark();
        bookmark.id = UUID.randomUUID().toString();
        bookmark.title = title;
        bookmark.sourceKey = currentSourceKey;
        bookmark.sourceTitle = currentSourceTitle;
        bookmark.caretPosition = caret;
        bookmark.selectionStart = selectionStart;
        bookmark.selectionEnd = selectionEnd;
        bookmark.previewText = preview;
        bookmark.type = bookmarkTypeForCurrentSource(typeOverride);
        bookmark.createdAt = System.currentTimeMillis();
        bookmark.updatedAt = bookmark.createdAt;
        saveSingleBookmarkForCurrentLocation(null);
        currentProfile.bookmarks.add(bookmark);
        saveData();
        refreshLibraryTree();
        updateHeader();
        log("Bookmark saved: " + bookmark.title);
    }

    private void saveBibleOverallBookmark(int caret, int selectionStart, int selectionEnd) {
        if (currentProfile == null || readerPane == null || currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return;
        if (currentProfile.bookmarks == null) currentProfile.bookmarks = new ArrayList<>();
        StudyBookmark bookmark = null;
        for (StudyBookmark b : currentProfile.bookmarks) {
            if (b != null && "BibleOverall".equals(b.type)) {
                bookmark = b;
                break;
            }
        }
        if (bookmark == null) {
            bookmark = new StudyBookmark();
            bookmark.id = UUID.randomUUID().toString();
            bookmark.createdAt = System.currentTimeMillis();
            currentProfile.bookmarks.add(bookmark);
        }
        bookmark.title = "Last Bible Reading Spot";
        bookmark.sourceKey = currentSourceKey;
        bookmark.sourceTitle = currentSourceTitle;
        bookmark.caretPosition = Math.max(0, Math.min(caret, readerPane.getDocument().getLength()));
        bookmark.selectionStart = selectionStart;
        bookmark.selectionEnd = selectionEnd;
        bookmark.previewText = previewAround(bookmark.caretPosition);
        bookmark.type = "BibleOverall";
        bookmark.updatedAt = System.currentTimeMillis();
        saveSingleBookmarkForCurrentLocation(bookmark);
        saveData();
        refreshLibraryTree();
    }


    private String bookmarkRowKeyForCurrentLocation() {
        return bookmarkRowKey(currentSourceKey, selectedBook, selectedChapter);
    }

    private String bookmarkRowKey(StudyBookmark bookmark) {
        if (bookmark == null) return "";
        return bookmarkRowKey(bookmark.sourceKey, "", 0);
    }

    private String bookmarkRowKey(String sourceKey, String book, int chapter) {
        String key = canonicalSourceKey(sourceKey);
        if (!safe(key).isEmpty()) return key;
        if (!safe(book).isEmpty() && chapter > 0) return safe(book) + "|" + chapter;
        return "";
    }

    private void saveSingleBookmarkForCurrentLocation(StudyBookmark keep) {
        if (currentProfile == null || currentProfile.bookmarks == null) return;
        String rowKey = getBookmarkScopeKeyForCurrentLocation();
        if (rowKey.isEmpty()) return;
        currentProfile.bookmarks.removeIf(existing -> existing != null
                && existing != keep
                && rowKey.equals(bookmarkRowKey(existing)));
    }

    private String bookmarkTypeForCurrentSource(String typeOverride) {
        if (typeOverride != null && !typeOverride.trim().isEmpty() && !"General".equals(typeOverride)) return typeOverride;
        if (currentSourceKey.startsWith("BIBLE:")) return "Bible";
        if (currentSourceKey.startsWith("LIBRARY:")) return "Library";
        return "General";
    }

    private String defaultBookmarkTitle(String sourceTitle, String preview) {
        String base = sourceTitle == null || sourceTitle.trim().isEmpty() ? "Bookmark" : sourceTitle.trim();
        return preview == null || preview.trim().isEmpty() ? base : base + " — " + shorten(preview.trim(), 55);
    }

    private String previewAround(int caret) {
        try {
            Document doc = readerPane.getDocument();
            int len = doc.getLength();
            int start = Math.max(0, caret - 70);
            int end = Math.min(len, caret + 110);
            return doc.getText(start, end - start).replaceAll("\\s+", " ").trim();
        } catch (Exception ignored) {
            return "";
        }
    }

    private void showBookmarksDialog() {
        if (currentProfile == null) return;
        if (currentProfile.bookmarks == null) currentProfile.bookmarks = new ArrayList<>();
        JDialog dialog = new JDialog(this, "Bookmarks", false);
        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.setBorder(new EmptyBorder(10, 10, 10, 10));
        content.setBackground(panelBg);

        bookmarkSearchField = new JTextField();
        bookmarkSearchField.setToolTipText("Filter bookmarks...");
        JPanel list = new JPanel();
        list.setLayout(new BoxLayout(list, BoxLayout.Y_AXIS));
        list.setBackground(panelBg);

        final Runnable[] renderBookmarksRef = new Runnable[1];
        renderBookmarksRef[0] = () -> {
            list.removeAll();
            String bookmarkQuery = bookmarkSearchField == null ? "" : bookmarkSearchField.getText().trim().toLowerCase(Locale.ROOT);
            List<StudyBookmark> bookmarks = new ArrayList<>(currentProfile.bookmarks);
            bookmarks.sort(Comparator.comparingLong((StudyBookmark b) -> b.createdAt).reversed());
            boolean any = false;
            for (StudyBookmark b : bookmarks) {
                if (b == null) continue;
                if (!bookmarkQuery.isEmpty() && !(safe(b.title) + " " + safe(b.sourceTitle) + " " + safe(b.previewText)).toLowerCase(Locale.ROOT).contains(bookmarkQuery)) continue;
                any = true;
                JPanel row = new JPanel(new BorderLayout(8, 4));
                row.setBorder(new CompoundBorder(new LineBorder(new Color(180, 145, 135)), new EmptyBorder(6, 6, 6, 6)));
                row.setBackground(cream);
                styleModernCard(row);
                JLabel info = new JLabel("<html><b>" + esc(safe(b.title)) + "</b><br>" + esc(safe(b.sourceTitle)) +
                        (safe(b.previewText).isEmpty() ? "" : "<br><i>" + esc(shorten(b.previewText, 160)) + "</i>") + "</html>");
                JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 5, 0));
                buttons.setOpaque(false);
                JButton open = blackButton("Open");
                open.addActionListener(e -> { openBookmark(b); dialog.dispose(); });
                JButton addToProject = blackButton("Add To Study Project");
                addToProject.addActionListener(e -> addBookmarkToStudyProject(b));
                JButton delete = blackButton("Delete");
                delete.addActionListener(e -> {
                    currentProfile.bookmarks.removeIf(existing -> existing != null && safe(existing.id).equals(safe(b.id)));
                    saveData();
                    refreshLibraryTree();
                    renderBookmarksRef[0].run();
                });
                buttons.add(open);
                buttons.add(addToProject);
                buttons.add(delete);
                row.add(info, BorderLayout.CENTER);
                row.add(buttons, BorderLayout.EAST);
                row.setAlignmentX(Component.LEFT_ALIGNMENT);
                list.add(row);
                list.add(Box.createVerticalStrut(6));
            }
            if (!any) {
                JLabel empty = new JLabel(bookmarkQuery.isEmpty() ? "No bookmarks saved yet." : "No bookmarks match your filter.");
                empty.setAlignmentX(Component.LEFT_ALIGNMENT);
                list.add(empty);
            }
            list.revalidate();
            list.repaint();
        };
        bookmarkSearchField.getDocument().addDocumentListener(new SimpleDocumentListener(renderBookmarksRef[0]));
        renderBookmarksRef[0].run();
        content.add(bookmarkSearchField, BorderLayout.NORTH);
        content.add(new JScrollPane(list), BorderLayout.CENTER);
        dialog.setContentPane(content);
        applyModernTheme(dialog);
        dialog.setMinimumSize(new Dimension(560, 360));
        dialog.setSize(620, 440);
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    private void goToBibleBookmark() {
        StudyBookmark newest = null;
        if (currentProfile != null && currentProfile.bookmarks != null) {
            for (StudyBookmark b : currentProfile.bookmarks) {
                if (b != null && "BibleOverall".equals(b.type) && (newest == null || b.createdAt > newest.createdAt)) newest = b;
            }
        }
        if (newest == null) {
            JOptionPane.showMessageDialog(this, "No Bible bookmark has been saved yet.");
            return;
        }
        openBookmark(newest);
    }

    private void openBookmark(StudyBookmark bookmark) {
    if (bookmark == null || bookmark.sourceKey == null || bookmark.sourceKey.trim().isEmpty()) return;

    openSourceKey(bookmark.sourceKey);

    SwingUtilities.invokeLater(() -> SwingUtilities.invokeLater(() -> {
        int len = readerPane.getDocument().getLength();

        if (bookmark.selectionStart >= 0 && bookmark.selectionEnd > bookmark.selectionStart && bookmark.selectionStart < len) {
            int s = Math.max(0, Math.min(bookmark.selectionStart, len));
            int e = Math.max(s, Math.min(bookmark.selectionEnd, len));
            readerPane.requestFocusInWindow();
            readerPane.select(s, e);
            scrollReaderToPosition(s);
        } else if (bookmark.hasViewportY) {
            scrollReaderToViewportY(bookmark.viewportY);
            int safeCaret = Math.max(0, Math.min(bookmark.caretPosition, len));
            readerPane.setCaretPosition(safeCaret);
        } else {
            moveReaderCaret(bookmark.caretPosition);
        }

        showSourceSummary(currentSourceKey, currentSourceTitle);
        showCard("study");
    }));
}

    private void openSourceKey(String sourceKey) {
        if (sourceKey.startsWith("BIBLE:")) {
            RefParts rp = refPartsFromBibleSourceKey(sourceKey);
            if (rp != null && data.bible.containsKey(rp.book)) {
                selectedBook = rp.book;
                selectedChapter = rp.chapter;
                refreshBookCombo();
                showSelectedChapter(false);
            }
        } else if (sourceKey.startsWith("LIBRARY:")) {
            showLibraryDoc(sourceKey.substring("LIBRARY:".length()));
        }
    }

    private RefParts refPartsFromBibleSourceKey(String sourceKey) {
        if (sourceKey == null || !sourceKey.startsWith("BIBLE:")) return null;
        String rest = sourceKey.substring("BIBLE:".length()).trim();
        RefParts rp = parseRef(rest + ":1");
        if (rp != null) return rp;
        Matcher chapterMatcher = Pattern.compile("^(.+):(\\d+)$").matcher(rest);
        if (chapterMatcher.matches()) {
            return parseRef(chapterMatcher.group(1).trim() + " " + chapterMatcher.group(2) + ":1");
        }
        return null;
    }


    private void scrollReaderToPosition(int position) {
        try {
            int len = readerPane.getDocument().getLength();
            int safePosition = Math.max(0, Math.min(position, len));
            Rectangle r = readerPane.modelToView2D(safePosition).getBounds();
            if (r != null) readerPane.scrollRectToVisible(r);
        } catch (Exception ignored) {}
    }
    private void scrollReaderToViewportY(int viewportY) {
    try {
        JViewport viewport = (JViewport) SwingUtilities.getAncestorOfClass(JViewport.class, readerPane);
        if (viewport == null) return;

        int maxY = Math.max(0, readerPane.getHeight() - viewport.getExtentSize().height);
        int safeY = Math.max(0, Math.min(viewportY, maxY));

        viewport.setViewPosition(new Point(0, safeY));
        readerPane.repaint();
    } catch (Exception ignored) {}
}

    private void moveReaderCaret(int position) {
        int len = readerPane.getDocument().getLength();
        int safePosition = Math.max(0, Math.min(position, len));
        readerPane.requestFocusInWindow();
        readerPane.setCaretPosition(safePosition);
        try {
            Rectangle r = readerPane.modelToView2D(safePosition).getBounds();
            if (r != null) readerPane.scrollRectToVisible(r);
        } catch (Exception ignored) {}
    }

    private void addMenu(JPopupMenu m, String label, Runnable r) {
        JMenuItem item = new JMenuItem(label);
        item.addActionListener(e -> r.run());
        m.add(item);
    }

    private boolean handleReaderLeftClick(MouseEvent e) {
        int pos = readerPane.viewToModel2D(e.getPoint());
        AnnotationBubbleMarker bubble = bubbleAt(pos);
        if (bubble != null) {
            hideSelectionActionPopup();
            if (e.getClickCount() >= 2) {
                if (openAnnotationPopups.isEmpty()) showAllAnnotationBubblePopups();
                else closeAllAnnotationBubblePopups();
            } else {
                toggleAnnotationBubblePopup(bubble);
            }
            return true;
        }
        if (isChapterTitleOffset(pos)) {
            showChapterTitleMenu(e.getX(), e.getY());
            return true;
        }
        java.util.List<TextAnnotation> annotations = getAnnotationsAtOffset(pos);
        if (!annotations.isEmpty()) {
            showAnnotationDetails(annotations);
            return true;
        }
        closeAllAnnotationBubblePopups();
        return false;
    }

    private void showQuickNoteForSelectionOrWord(MouseEvent e) {
        if (readerPane.getSelectionEnd() <= readerPane.getSelectionStart()) {
            selectWordAt(readerPane.viewToModel2D(e.getPoint()));
        }
        if (readerPane.getSelectionEnd() > readerPane.getSelectionStart()) addAnnotationFromSelection("Note", "");
    }

    private void selectWordAt(int pos) {
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            if (pos < 0 || pos >= text.length()) return;
            int s = pos;
            int e = pos;
            while (s > 0 && Character.isLetterOrDigit(text.charAt(s - 1))) s--;
            while (e < text.length() && Character.isLetterOrDigit(text.charAt(e))) e++;
            if (e > s) readerPane.select(s, e);
        } catch (Exception ignored) {}
    }

    private TextAnnotation annotationAt(int pos) {
        if (currentProfile == null || currentSourceKey == null) return null;
        int sourcePos = renderedOffsetToSourceOffset(pos);
        TextAnnotation best = null;
        for (TextAnnotation a : currentProfile.annotations) {
            if (!a.wholeChapter && currentSourceKey.equals(a.sourceKey) && sourcePos >= a.start && sourcePos < a.end) {
                if (best == null || (a.end - a.start) < (best.end - best.start)) best = a;
            }
        }
        return best;
    }

    private void toggleAnnotationBubblePopup(AnnotationBubbleMarker bubble) {
        if (bubble == null || bubble.annotation == null) return;
        String id = bubble.popupKey();
        JPopupMenu existing = openAnnotationPopups.get(id);
        if (existing != null && existing.isVisible()) {
            existing.setVisible(false);
            openAnnotationPopups.remove(id);
            return;
        }
        showAnnotationBubblePopup(bubble);
    }

    private void showAllAnnotationBubblePopups() {
        for (AnnotationBubbleMarker bubble : visibleAnnotationBubbles) showAnnotationBubblePopup(bubble);
    }

    private void closeAllAnnotationBubblePopups() {
        for (JPopupMenu popup : new ArrayList<>(openAnnotationPopups.values())) {
            if (popup != null) popup.setVisible(false);
        }
        openAnnotationPopups.clear();
    }

    private void showAnnotationBubblePopup(AnnotationBubbleMarker bubble) {
        if (bubble == null || bubble.annotation == null) return;
        String popupKey = bubble.popupKey();
        JPopupMenu old = openAnnotationPopups.remove(popupKey);
        if (old != null) old.setVisible(false);
        JPopupMenu popup = new JPopupMenu();
        popup.setBorder(new CompoundBorder(new LineBorder(modernBorder), new EmptyBorder(6, 6, 6, 6)));
        popup.add(buildAnnotationPopupContent(bubble.annotations));
        popup.addPopupMenuListener(new PopupMenuListener() {
            public void popupMenuWillBecomeVisible(PopupMenuEvent e) {}
            public void popupMenuWillBecomeInvisible(PopupMenuEvent e) { openAnnotationPopups.remove(popupKey); }
            public void popupMenuCanceled(PopupMenuEvent e) { openAnnotationPopups.remove(popupKey); }
        });
        try {
            Rectangle r = readerPane.modelToView2D(bubble.startOffset).getBounds();
            popup.show(readerPane, r.x + Math.max(18, r.width + 4), r.y);
        } catch (Exception ex) {
            popup.show(readerPane, 24, 24);
        }
        openAnnotationPopups.put(popupKey, popup);
    }

    private JComponent buildAnnotationPopupContent(TextAnnotation a) {
        JPanel panel = new JPanel(new BorderLayout(8, 8));
        panel.setBackground(modernSurface);
        panel.setPreferredSize(new Dimension(320, Math.min(240, Math.max(145, 120 + safe(a.note).length() / 3))));
        panel.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() >= 2) closeAllAnnotationBubblePopups();
            }
        });

        JLabel title = new JLabel(annotationDisplayName(a) + (a.wholeChapter ? " — Whole Chapter" : "") + (safe(a.category).isEmpty() || "Question".equals(a.type) ? "" : " — " + safe(a.category)));
        title.setFont(modernBoldFont);
        title.setForeground("Question".equals(a.type) ? modernDanger : modernDarkRed);
        panel.add(title, BorderLayout.NORTH);

        JComponent center = "Question".equals(a.type) ? buildQuestionPopupBody(a) : buildPlainAnnotationBody(a);
        panel.add(center, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 0));
        actions.setOpaque(false);
        JButton details = new JButton("Details");
        details.addActionListener(e -> { showAnnotationDetails(a); closeAllAnnotationBubblePopups(); });
        JButton edit = new JButton("Edit");
        edit.addActionListener(e -> { closeAllAnnotationBubblePopups(); editAnnotation(a); });
        JButton delete = new JButton("Delete");
        delete.addActionListener(e -> { closeAllAnnotationBubblePopups(); deleteAnnotation(a); });
        actions.add(details);
        actions.add(edit);
        if (!safe(a.target).isEmpty()) {
            JButton open = new JButton("Open");
            open.addActionListener(e -> { closeAllAnnotationBubblePopups(); openAnnotationTarget(a); });
            actions.add(open);
        }
        actions.add(delete);
        panel.add(actions, BorderLayout.SOUTH);
        return panel;
    }



    private JComponent buildPlainAnnotationBody(TextAnnotation a) {
        JTextArea body = new JTextArea(safe(a.note).isEmpty() ? "(No note text)" : a.note);
        body.setEditable(false);
        body.setLineWrap(true);
        body.setWrapStyleWord(true);
        body.setFont(modernBaseFont);
        body.setBackground(modernSurface);
        body.setBorder(new EmptyBorder(2, 2, 2, 2));
        return new JScrollPane(body);
    }

    private JComponent buildQuestionPopupBody(TextAnnotation a) {
        StudyQuestion q = questionForAnnotation(a.id);
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
        panel.setBackground(modernSurface);
        JTextArea question = readonlyArea();
        question.setBackground(modernSurface);
        question.setText(questionTypeDisplay(q == null ? questionTypeForAnnotation(a) : q.questionType) + "\n"
                + (q != null && q.answered ? "Answered" : "Unanswered") + " • " + (q == null ? 0 : q.answers.size()) + " answer(s)\n\n"
                + safe(a.note));
        panel.add(question);
        if (q != null) {
            for (QuestionAnswer ans : q.answers) {
                JTextArea answer = readonlyArea();
                answer.setBackground(new Color(255, 252, 247));
                answer.setBorder(new CompoundBorder(new MatteBorder(1, 0, 0, 0, modernBorder), new EmptyBorder(5, 2, 5, 2)));
                answer.setText("Answer — " + displayDate(ans.createdAt) + (ans.updatedAt > ans.createdAt ? " (edited " + displayDate(ans.updatedAt) + ")" : "") + "\n" + safe(ans.text));
                panel.add(answer);
            }
            JTextArea newAnswer = new JTextArea(3, 28);
            newAnswer.setLineWrap(true);
            newAnswer.setWrapStyleWord(true);
            JButton add = new JButton("Add Answer");
            add.addActionListener(e -> {
                String text = newAnswer.getText().trim();
                if (text.isEmpty()) return;
                addAnswerToQuestion(q, text);
                closeAllAnnotationBubblePopups();
                refreshQuestions();
                reloadCurrentSource();
            });
            JPanel addPanel = new JPanel(new BorderLayout(4, 4));
            addPanel.setOpaque(false);
            addPanel.setBorder(new EmptyBorder(6, 0, 0, 0));
            addPanel.add(new JScrollPane(newAnswer), BorderLayout.CENTER);
            addPanel.add(add, BorderLayout.SOUTH);
            panel.add(addPanel);
        }
        return new JScrollPane(panel);
    }

    private JComponent buildAnnotationPopupContent(java.util.List<TextAnnotation> annotations) {
        if (annotations == null || annotations.isEmpty()) return buildAnnotationPopupContent(new TextAnnotation("", "", 0, 0, "", "Note", "", "", ""));
        if (annotations.size() == 1) return buildAnnotationPopupContent(annotations.get(0));

        JPanel panel = new JPanel(new BorderLayout(8, 8));
        panel.setBackground(modernSurface);
        panel.setPreferredSize(new Dimension(380, 300));
        panel.addMouseListener(new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() >= 2) closeAllAnnotationBubblePopups();
            }
        });

        TextAnnotation primary = primaryAnnotation(annotations);
        JLabel title = new JLabel("💬 " + annotations.size() + " annotations on this text");
        title.setFont(modernBoldFont);
        title.setForeground(modernDarkRed);
        panel.add(title, BorderLayout.NORTH);

        JPanel list = new JPanel();
        list.setLayout(new BoxLayout(list, BoxLayout.Y_AXIS));
        list.setBackground(modernSurface);
        for (TextAnnotation a : annotations) {
            JPanel card = new JPanel(new BorderLayout(5, 5));
            card.setBackground(new Color(255, 252, 247));
            card.setBorder(new CompoundBorder(new MatteBorder(0, 5, 0, 0, colorForAnnotation(a)), new EmptyBorder(6, 8, 6, 6)));
            JLabel label = new JLabel(annotationDisplayName(a) + (safe(a.category).isEmpty() ? "" : " — " + safe(a.category)));
            label.setFont(modernBoldFont);
            card.add(label, BorderLayout.NORTH);
            JTextArea body = new JTextArea(annotationSummaryText(a));
            body.setEditable(false);
            body.setLineWrap(true);
            body.setWrapStyleWord(true);
            body.setFont(modernBaseFont);
            body.setBackground(card.getBackground());
            card.add(body, BorderLayout.CENTER);
            JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 0));
            actions.setOpaque(false);
            JButton details = new JButton("Details");
            details.addActionListener(e -> { showAnnotationDetails(a); closeAllAnnotationBubblePopups(); });
            JButton edit = new JButton("Edit");
            edit.addActionListener(e -> { closeAllAnnotationBubblePopups(); editAnnotation(a); });
            actions.add(details);
            actions.add(edit);
            if (!safe(a.target).isEmpty()) {
                JButton open = new JButton("Open");
                open.addActionListener(e -> { closeAllAnnotationBubblePopups(); openAnnotationTarget(a); });
                actions.add(open);
            }
            card.add(actions, BorderLayout.SOUTH);
            list.add(card);
            list.add(Box.createVerticalStrut(6));
        }
        panel.add(new JScrollPane(list), BorderLayout.CENTER);
        JButton allDetails = new JButton("Show All Details");
        allDetails.addActionListener(e -> { showAnnotationDetails(annotations); closeAllAnnotationBubblePopups(); });
        JPanel bottom = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 0));
        bottom.setOpaque(false);
        bottom.add(allDetails);
        if (primary != null) {
            JButton select = new JButton("Select Text");
            select.addActionListener(e -> { safeSelect(primary.start, primary.end); closeAllAnnotationBubblePopups(); });
            bottom.add(select);
        }
        panel.add(bottom, BorderLayout.SOUTH);
        return panel;
    }

    private String annotationDisplayName(TextAnnotation a) {
        String type = safe(a == null ? "" : a.type);
        if (type.isEmpty()) type = "Annotation";
        return type + " Highlight";
    }

    private String annotationSummaryText(TextAnnotation a) {
        if (a == null) return "";
        StringBuilder sb = new StringBuilder();
        if (!safe(a.target).isEmpty()) sb.append("Target: ").append(a.target).append("\n");
        if (!safe(a.note).isEmpty()) sb.append(a.note);
        if (sb.length() == 0) sb.append("(No note text)");
        return sb.toString();
    }

    private Integer verseNumberAtPosition(int pos) {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return null;
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            if (pos < 0 || pos >= text.length()) return null;

            int lineStart = text.lastIndexOf('\n', Math.max(0, pos - 1));
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            int lineEnd = text.indexOf('\n', pos);
            if (lineEnd < 0) lineEnd = text.length();

            int i = lineStart;
            while (i < lineEnd && Character.isWhitespace(text.charAt(i))) i++;
            int numStart = i;
            while (i < lineEnd && Character.isDigit(text.charAt(i))) i++;
            int numEnd = i;

            if (numEnd <= numStart) return null;
            if (pos < numStart || pos > numEnd) return null;
            if (numEnd < lineEnd && !Character.isWhitespace(text.charAt(numEnd))) return null;

            return Integer.parseInt(text.substring(numStart, numEnd));
        } catch (Exception ignored) {
            return null;
        }
    }


    private Integer verseNumberContainingPosition(int pos) {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return null;
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            if (pos < 0 || pos > text.length()) return null;
            if (pos == text.length() && pos > 0) pos--;

            int lineStart = text.lastIndexOf('\n', Math.max(0, pos - 1));
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            int lineEnd = text.indexOf('\n', pos);
            if (lineEnd < 0) lineEnd = text.length();

            int i = lineStart;
            while (i < lineEnd && Character.isWhitespace(text.charAt(i))) i++;
            int numStart = i;
            while (i < lineEnd && Character.isDigit(text.charAt(i))) i++;
            int numEnd = i;

            if (numEnd <= numStart) return null;
            if (numEnd < lineEnd && !Character.isWhitespace(text.charAt(numEnd))) return null;
            if (pos < numStart || pos > lineEnd) return null;

            int verse = Integer.parseInt(text.substring(numStart, numEnd));
            if (!data.getVerses(selectedBook, selectedChapter).containsKey(verse)) return null;
            return verse;
        } catch (Exception ignored) {
            return null;
        }
    }

    private String greekKeyForSelection() {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return null;
        if (readerPane == null) return null;
        int[] range = readerSelectionSourceRange();
        int start = range[0];
        int end = range[1];
        if (end <= start || readerPane.getSelectedText() == null || readerPane.getSelectedText().trim().isEmpty()) return null;

        Integer verse = verseNumberContainingPosition(sourceOffsetToRenderedOffset(start, true));
        if (verse == null) verse = verseNumberContainingPosition(sourceOffsetToRenderedOffset(Math.max(start, end - 1), true));
        if (verse == null) return null;
        return selectedBook + " " + selectedChapter + ":" + verse;
    }

    private void showGreekForCurrentSelection() {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) {
            JOptionPane.showMessageDialog(this, "Greek lookup is available when selected text is in a Bible chapter.");
            return;
        }
        String key = greekKeyForSelection();
        if (key == null) {
            JOptionPane.showMessageDialog(this, "Select text in a Bible verse first, then choose View Greek For This Verse.");
            return;
        }
        showGreekForVerse(key);
    }

    private void searchSelectedTextInGreek() {
        String selected = readerPane == null ? "" : safe(readerPane.getSelectedText()).trim();
        if (selected.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Select a word or phrase first.");
            return;
        }

        if (containsGreekCharacters(selected)) {
            if (greekSearchField != null) {
                greekSearchField.setText(selected);
                doGreekSearch();
            }
            showCard("greekSearch");
            return;
        }

        String key = greekKeyForSelection();
        if (key == null) {
            JOptionPane.showMessageDialog(this, "Select text inside a Bible verse to open that verse's Greek details.");
            return;
        }
        showGreekDetailsInSidebar(key);
        statusLabel.setText(" Greek details for " + key + " opened from selected text: " + shorten(selected, 60));
    }

    private boolean containsGreekCharacters(String s) {
        if (s == null) return false;
        for (int i = 0; i < s.length(); i++) {
            Character.UnicodeBlock block = Character.UnicodeBlock.of(s.charAt(i));
            if (block == Character.UnicodeBlock.GREEK || block == Character.UnicodeBlock.GREEK_EXTENDED) return true;
        }
        return false;
    }

    private int[] verseRangeInReader(int verse) {
        try {
            String txt = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            String marker = "\n" + verse + " ";
            int idx = txt.indexOf(marker);
            if (idx < 0 && txt.startsWith(verse + " ")) idx = 0;
            if (idx < 0) return new int[]{0, 0};
            int start = idx == 0 ? 0 : idx + 1;
            int end = txt.indexOf("\n\n", start);
            if (end < 0) end = txt.length();
            return new int[]{start, end};
        } catch (Exception ignored) {
            return new int[]{0, 0};
        }
    }

    private void showGreekForVerse(String key) {
        showGreekDialog(key, false);
    }

    private void showGreekDetailsInSidebar(String key) {
        JPanel panel = ensureDetailsPanel();
        GreekEntry ge = data.greek.get(key);
        String greekText = ge == null ? "" : ge.greekText;
        String englishText = englishVerseTextForKey(key);
        String details = ge == null
                ? "No Greek entry imported for this verse. Use Import > Download + Import MorphGNT Greek, import a MorphGNT ZIP/TXT folder, or import a Greek CSV."
                : ge.details;

        panel.removeAll();
        addDetailTitle("Greek Details");
        addDetailText("Reference: " + key);
        addDetailText("English verse text:\n" + (englishText.isEmpty() ? "(English verse not found in the current Bible text.)" : englishText));
        addDetailText("Greek text:\n" + (greekText.isEmpty() ? "(No Greek text imported.)" : greekText));
        addDetailText("Morphology/details:\n" + (details.isEmpty() ? "(No morphology/details imported.)" : details));

        JButton open = blackButton("Open Verse");
        open.setAlignmentX(Component.LEFT_ALIGNMENT);
        open.addActionListener(e -> openGreekResultVerse(key, false));

        JButton note = blackButton("Add Greek Note");
        note.setAlignmentX(Component.LEFT_ALIGNMENT);
        note.addActionListener(e -> addGreekNoteFromSearchResult(key));

        JButton copy = blackButton("Copy Greek Text");
        copy.setAlignmentX(Component.LEFT_ALIGNMENT);
        copy.addActionListener(e -> copyGreekTextToClipboard(key));

        JButton addTopic = blackButton("Add Greek to Topic Page");
        addTopic.setAlignmentX(Component.LEFT_ALIGNMENT);
        addTopic.addActionListener(e -> addLinkedItemToTopicPage(new LinkedItem("GREEK", key, "related")));

        panel.add(open);
        panel.add(Box.createVerticalStrut(6));
        panel.add(note);
        panel.add(Box.createVerticalStrut(6));
        panel.add(copy);
        panel.add(Box.createVerticalStrut(6));
        panel.add(addTopic);
        panel.revalidate();
        panel.repaint();
        statusLabel.setText(" Showing Greek details for " + key);
    }

    private void openGreekResultVerse(String key, boolean showDetails) {
        openTarget(key);
        RefParts rp = parseRef(key);
        if (rp != null) selectVerseText(rp.verse);
        if (showDetails) showGreekDetailsInSidebar(key);
    }

    private void addGreekNoteFromSearchResult(String key) {
        openGreekResultVerse(key, true);
        showGreekDialog(key, false);
    }

    private void copyGreekTextToClipboard(String key) {
        GreekEntry ge = data.greek.get(key);
        String greekText = ge == null ? "" : ge.greekText;
        StringSelection selection = new StringSelection(greekText);
        Toolkit.getDefaultToolkit().getSystemClipboard().setContents(selection, selection);
        statusLabel.setText(" Copied Greek text for " + key);
    }

    private void addGreekSelectionToTopicPage() {
        String key = greekKeyForSelection();
        if (key == null) {
            JOptionPane.showMessageDialog(this, "Select text in a Bible verse first.");
            return;
        }
        addLinkedItemToTopicPage(new LinkedItem("GREEK", key, "related"));
    }

    private void addGreekNoteForSelectionOrVerse() {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) {
            JOptionPane.showMessageDialog(this, "Greek notes are available when selected text is in a Bible chapter.");
            return;
        }

        String key = greekKeyForSelection();
        if (key == null) {
            int caret = readerPane.getCaretPosition();
            Integer verse = verseNumberContainingPosition(caret);
            if (verse == null) verse = verseNumberContainingPosition(Math.max(0, caret - 1));
            if (verse != null) key = selectedBook + " " + selectedChapter + ":" + verse;
        }
        if (key == null) {
            JOptionPane.showMessageDialog(this, "Select text in a Bible verse, or place the cursor inside a verse, before adding a Greek note.");
            return;
        }

        showGreekDialog(key, true);
    }

    private void showGreekDialog(String key, boolean saveSelectionWhenPossible) {
        GreekEntry ge = data.greek.get(key);
        RefParts rp = parseRef(key);
        String englishText = englishVerseTextForKey(key);
        String greekText = ge == null ? "" : ge.greekText;
        String details = ge == null
                ? "No Greek entry imported for this verse. Use Import > Download + Import MorphGNT Greek, import a MorphGNT ZIP/TXT folder, or import a Greek CSV."
                : ge.details;

        JTextPane info = new JTextPane();
        info.setFont(new Font("Consolas", Font.PLAIN, 13));
        info.setEditable(false);
        info.setText(greekDialogText(key, englishText, greekText, details));
        info.setCaretPosition(0);

        JTextField search = new JTextField();
        search.setFont(new Font("Segoe UI", Font.PLAIN, 13));
        JLabel searchCount = new JLabel(" ");
        searchCount.setForeground(new Color(90, 70, 60));
        Runnable highlightSearch = () -> highlightGreekDialogMatches(info, search.getText(), searchCount);
        search.getDocument().addDocumentListener(new SimpleDocumentListener(highlightSearch));

        JPanel searchPanel = new JPanel(new BorderLayout(5, 5));
        searchPanel.add(new JLabel("Search Greek/details:"), BorderLayout.WEST);
        searchPanel.add(search, BorderLayout.CENTER);
        searchPanel.add(searchCount, BorderLayout.EAST);

        JTextArea note = new JTextArea(5, 58);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        JPanel notePanel = new JPanel(new BorderLayout(5, 5));
        notePanel.add(new JLabel(saveSelectionWhenPossible
                ? "Optional Greek note to save on selected phrase (or whole verse if no phrase is selected):"
                : "Optional Greek note to save on this verse:"), BorderLayout.NORTH);
        notePanel.add(new JScrollPane(note), BorderLayout.CENTER);

        JPanel center = new JPanel(new BorderLayout(6, 6));
        center.add(searchPanel, BorderLayout.NORTH);
        center.add(new JScrollPane(info), BorderLayout.CENTER);
        center.add(notePanel, BorderLayout.SOUTH);
        center.setBorder(new EmptyBorder(10, 10, 10, 10));

        JDialog dialog = new JDialog(this, "Greek Lookup - " + key, true);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.setLayout(new BorderLayout(6, 6));
        dialog.add(center, BorderLayout.CENTER);

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 8));
        JButton copyGreek = blackButton("Copy Greek Text");
        JButton openVerse = blackButton("Open Verse");
        JButton saveNote = blackButton("Add Greek Note");
        JButton close = blackButton("Close");

        copyGreek.addActionListener(e -> {
            StringSelection selection = new StringSelection(greekText == null ? "" : greekText);
            Toolkit.getDefaultToolkit().getSystemClipboard().setContents(selection, selection);
            statusLabel.setText(" Copied Greek text for " + key);
        });
        openVerse.addActionListener(e -> {
            openGreekResultVerse(key, false);
            dialog.dispose();
        });
        saveNote.addActionListener(e -> {
            String noteText = note.getText().trim();
            if (noteText.isEmpty()) {
                JOptionPane.showMessageDialog(dialog, "Write a Greek note before saving.");
                return;
            }
            TextAnnotation saved = saveGreekNoteAnnotation(key, rp, noteText, saveSelectionWhenPossible);
            if (saved != null) {
                dialog.dispose();
                showAnnotationDetails(saved);
            }
        });
        close.addActionListener(e -> dialog.dispose());

        buttons.add(copyGreek);
        buttons.add(openVerse);
        buttons.add(saveNote);
        buttons.add(close);
        dialog.add(buttons, BorderLayout.SOUTH);
        applyModernTheme(dialog);
        dialog.setMinimumSize(new Dimension(680, 500));
        dialog.setSize(760, 620);
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    private String greekDialogText(String key, String englishText, String greekText, String details) {
        return "Reference\n" + key +
                "\n\nEnglish verse text\n" + (englishText == null || englishText.isEmpty() ? "(English verse not found in the current Bible text.)" : englishText) +
                "\n\nGreek text\n" + (greekText == null || greekText.isEmpty() ? "(No Greek text imported.)" : greekText) +
                "\n\nMorphology/details\n" + (details == null || details.isEmpty() ? "(No morphology/details imported.)" : details);
    }

    private void highlightGreekDialogMatches(JTextPane info, String query, JLabel countLabel) {
        highlightTextPaneMatches(info, query, countLabel);
    }

    private void highlightTextPaneMatches(JTextPane info, String query, JLabel countLabel) {
        Highlighter highlighter = info.getHighlighter();
        highlighter.removeAllHighlights();
        String q = query == null ? "" : query.trim().toLowerCase(Locale.ROOT);
        if (q.isEmpty()) {
            if (countLabel != null) countLabel.setText(" ");
            return;
        }
        String haystack = info.getText().toLowerCase(Locale.ROOT);
        Highlighter.HighlightPainter painter = new DefaultHighlighter.DefaultHighlightPainter(new Color(255, 235, 130));
        int count = 0;
        int idx = haystack.indexOf(q);
        while (idx >= 0 && count < 100) {
            try {
                highlighter.addHighlight(idx, idx + q.length(), painter);
            } catch (BadLocationException ignored) {}
            count++;
            idx = haystack.indexOf(q, idx + q.length());
        }
        if (countLabel != null) countLabel.setText(count + (count == 1 ? " match" : " matches"));
    }

    private TextAnnotation saveGreekNoteAnnotation(String key, RefParts rp, String noteText, boolean useSelectionWhenPossible) {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) {
            JOptionPane.showMessageDialog(this, "Open the Bible chapter before saving a Greek note.");
            return null;
        }
        if (rp == null) rp = parseRef(key);
        int[] range = greekNoteRange(rp, useSelectionWhenPossible);
        String selected = key;
        try {
            if (range[1] > range[0]) selected = readerSelectedPlainText(range[0], range[1]);
        } catch (Exception ignored) {}

        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, range[0], range[1], selected, "Greek", "", noteText, key);
        currentProfile.annotations.add(a);
        saveData();
        refreshRecentNotes();
        reloadCurrentSource();
        return a;
    }

    private int[] greekNoteRange(RefParts rp, boolean useSelectionWhenPossible) {
        if (useSelectionWhenPossible) {
            int[] range = readerSelectionSourceRange();
            int start = range[0];
            int end = range[1];
            String selected = readerPane.getSelectedText();
            if (end > start && selected != null && !selected.trim().isEmpty()) {
                Integer selectionVerse = verseNumberContainingPosition(sourceOffsetToRenderedOffset(start, true));
                if (selectionVerse == null) selectionVerse = verseNumberContainingPosition(sourceOffsetToRenderedOffset(Math.max(start, end - 1), true));
                if (selectionVerse != null && (rp == null || selectionVerse == rp.verse)) return new int[]{start, end};
            }
        }
        return rp == null ? new int[]{0, 0} : renderedRangeToSourceRange(verseRangeInReader(rp.verse));
    }

    private int[] renderedRangeToSourceRange(int[] range) {
        if (range == null || range.length < 2) return new int[]{0, 0};
        int start = renderedOffsetToSourceOffset(range[0]);
        int end = renderedOffsetToSourceOffset(range[1]);
        if (end < start) end = start;
        return new int[]{start, end};
    }

    private String englishVerseTextFromData(String key) {
        Verse v = data.findVerse(key);
        return v == null ? "" : v.text;
    }

    private String englishVerseTextForKey(String key) {
        RefParts rp = parseRef(key);
        if (rp == null) return "";
        Verse v = data.findVerse(key);
        if (v != null) return v.text;
        int[] range = verseRangeInReader(rp.verse);
        try {
            if (range[1] > range[0]) return readerPane.getDocument().getText(range[0], range[1] - range[0]).trim();
        } catch (Exception ignored) {}
        return "";
    }

    private int[] readerSelectionSourceRange() {
        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        int sourceStart = renderedOffsetToSourceOffset(Math.min(start, end));
        int sourceEnd = renderedOffsetToSourceOffset(Math.max(start, end));
        if (sourceEnd < sourceStart) sourceEnd = sourceStart;
        return new int[]{sourceStart, sourceEnd};
    }

    private String readerSelectedPlainText(int sourceStart, int sourceEnd) {
        try {
            int renderedStart = sourceOffsetToRenderedOffset(sourceStart, true);
            int renderedEnd = sourceOffsetToRenderedOffset(sourceEnd, false);
            String selected = readerPane.getDocument().getText(renderedStart, Math.max(0, renderedEnd - renderedStart));
            for (AnnotationBubbleMarker marker : visibleAnnotationBubbles) {
                selected = selected.replace(marker.markerText, "");
            }
            return selected;
        } catch (Exception ignored) {
            return safe(readerPane.getSelectedText());
        }
    }

    private void addAnnotationFromSelection(String type, String category) {
        int[] range = readerSelectionSourceRange();
        int start = range[0];
        int end = range[1];
        if (end <= start) {
            JOptionPane.showMessageDialog(this, "Highlight/select text first, then right-click it.");
            return;
        }

        String selected = readerSelectedPlainText(start, end);
        JTextArea note = new JTextArea(8, 44);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        int r = JOptionPane.showConfirmDialog(this, new JScrollPane(note), "Add " + type + " to: " + shorten(selected, 70), JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        String body = note.getText().trim();
        if (body.isEmpty() && !"Category".equals(type)) return;

        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, start, end, selected, type, category, body, "");
        currentProfile.annotations.add(a);

        if ("Question".equals(type)) {
            currentProfile.questions.add(new StudyQuestion(a.id, currentSourceTitle, selected, body, normalizeQuestionType(category), currentSourceKey, currentSourceTitle, selectedBook, selectedChapter, false));
        }

        saveData();
        refreshRecentNotes();
        refreshPinnedItems();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void addCategoryFromSelection() {
        String cat = chooseOrCreateCategory();
        if (cat == null || cat.trim().isEmpty()) return;

        int[] range = readerSelectionSourceRange();
        int start = range[0];
        int end = range[1];
        if (end <= start) return;

        String selected = readerSelectedPlainText(start, end);
        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, start, end, selected, "Category", cat, "Added to category: " + cat, "");
        currentProfile.annotations.add(a);

        saveData();
        refreshCategories();
        refreshRecentNotes();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private String chooseOrCreateCategory() {
        ensureCategoryColors();

        Object[] opts = new Object[currentProfile.categories.size() + 1];
        opts[0] = "+ Create New Category";
        int i = 1;
        for (String c : currentProfile.categories.keySet()) opts[i++] = c;

        Object o = JOptionPane.showInputDialog(this, "Choose category:", "Category", JOptionPane.PLAIN_MESSAGE, null, opts, opts[0]);
        if (o == null) return null;

        String cat = o.toString();
        if (cat.startsWith("+")) return createCategory(null);
        return cat;
    }

    private void addAttachmentFromSelection() {
        int[] range = readerSelectionSourceRange();
        int start = range[0];
        int end = range[1];
        if (end <= start) return;
        String selected = readerSelectedPlainText(start, end);

        JTextField bibleRef = new JTextField();
        JComboBox<String> docBox = new JComboBox<>();
        docBox.addItem("");
        for (LibraryDoc d : data.libraryDocs) docBox.addItem(d.title);

        JTextArea chunk = new JTextArea(4, 42);
        chunk.setLineWrap(true);
        chunk.setWrapStyleWord(true);

        JTextField relation = new JTextField("Cross-reference");
        JTextArea note = new JTextArea(5, 42);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        JPanel p = new JPanel(new GridLayout(0, 1, 5, 5));
        p.add(new JLabel("Attach to Bible ref, e.g. Romans 14:13. Leave blank if attaching to a book chunk."));
        p.add(bibleRef);
        p.add(new JLabel("OR attach to a selected imported book:"));
        p.add(docBox);
        p.add(new JLabel("Exact chunk from that book. The app will accept it only if it appears exactly once:"));
        p.add(new JScrollPane(chunk));
        p.add(new JLabel("Relationship label:"));
        p.add(relation);
        p.add(new JLabel("Note:"));
        p.add(new JScrollPane(note));

        int r = JOptionPane.showConfirmDialog(this, p, "Attach Selected Text", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        String target;
        String ref = bibleRef.getText().trim();
        if (!ref.isEmpty()) {
            RefParts rp = parseRef(ref);
            if (rp == null || data.findVerse(rp.key()) == null) {
                JOptionPane.showMessageDialog(this, "I could not find that Bible verse. Use a format like Romans 14:13.");
                return;
            }
            target = rp.key();
        } else {
            String docTitle = Objects.toString(docBox.getSelectedItem(), "").trim();
            String chunkText = chunk.getText().trim();
            if (docTitle.isEmpty() || chunkText.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Enter a Bible ref or choose a book and enter a unique chunk.");
                return;
            }
            LibraryDoc d = data.findLibraryDoc(docTitle);
            if (d == null) return;
            int first = normalizeForFind(d.body).indexOf(normalizeForFind(chunkText));
            if (first < 0) {
                JOptionPane.showMessageDialog(this, "That chunk was not found in the selected book.");
                return;
            }
            int second = normalizeForFind(d.body).indexOf(normalizeForFind(chunkText), first + 1);
            if (second >= 0) {
                JOptionPane.showMessageDialog(this, "That chunk appears more than once. Add more words until it only appears once.");
                return;
            }
            target = "LIBRARY:" + docTitle + "::" + chunkText;
        }

        String body = "Relationship: " + relation.getText().trim() + "\n" + note.getText().trim();
        TextAnnotation a = new TextAnnotation(currentSourceKey, currentSourceTitle, start, end, selected, "Link", "", body, target);
        currentProfile.annotations.add(a);
        saveData();
        refreshRecentNotes();
        refreshMarginNotesPanel();
        refreshCategories();
        refreshStudyProjects();
        refreshTopicPages();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void changeAnnotationCategory(TextAnnotation a) {
        String newCategory = chooseOrCreateCategory();
        if (newCategory == null || newCategory.trim().isEmpty()) return;

        newCategory = newCategory.trim();
        a.category = newCategory;
        a.note = "Added to category: " + newCategory;
        touchAnnotation(a);

        saveData();
        refreshCategories();
        refreshRecentNotes();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void removeAnnotationFromCategory(TextAnnotation a) {
        int choice = JOptionPane.showConfirmDialog(
                this,
                "Remove this highlight from the category?",
                "Remove From Category",
                JOptionPane.YES_NO_OPTION);
        if (choice != JOptionPane.YES_OPTION) return;

        currentProfile.annotations.removeIf(x -> x.id.equals(a.id));
        currentProfile.questions.removeIf(q -> q.annotationId.equals(a.id));
        saveData();
        refreshRecentNotes();
        reloadCurrentSource();
        refreshCategories();
        showSourceSummary(currentSourceKey, currentSourceTitle);
    }

    private void editAnnotation(TextAnnotation a) {
        JTextField type = new JTextField(a.type);
        JTextField cat = new JTextField(a.category);
        JTextField target = new JTextField(a.target);
        JTextArea note = new JTextArea(a.note, 8, 44);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        JPanel p = new JPanel(new GridLayout(0, 1, 5, 5));
        p.add(new JLabel("Type:")); p.add(type);
        p.add(new JLabel("Category:")); p.add(cat);
        p.add(new JLabel("Target / attachment:")); p.add(target);
        p.add(new JLabel("Note:")); p.add(new JScrollPane(note));

        int r = JOptionPane.showConfirmDialog(this, p, "Edit Highlight", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        a.type = type.getText().trim().isEmpty() ? "Note" : type.getText().trim();
        a.category = cat.getText().trim();
        a.target = target.getText().trim();
        a.note = note.getText().trim();
        touchAnnotation(a);

        saveData();
        refreshRecentNotes();
        refreshMarginNotesPanel();
        refreshCategories();
        refreshStudyProjects();
        refreshTopicPages();
        reloadCurrentSource();
        showAnnotationDetails(a);
    }

    private void deleteAnnotation(TextAnnotation a) {
        if (a == null || currentProfile == null) return;
        if (JOptionPane.showConfirmDialog(this, "Delete this note? This cannot be undone.", "Delete Note", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE) != JOptionPane.YES_OPTION) return;
        String annotationId = safe(a.id);
        currentProfile.annotations.removeIf(x -> x != null && annotationId.equals(safe(x.id)));
        currentProfile.questions.removeIf(q -> q != null && annotationId.equals(safe(q.annotationId)));
        saveData();
        refreshRecentNotes();
        refreshMarginNotesPanel();
        refreshCategories();
        refreshStudyProjects();
        refreshTopicPages();
        reloadCurrentSource();
        showSourceSummary(currentSourceKey, currentSourceTitle);
    }

    private void reloadCurrentSource() {
        reloadCurrentSource(true);
    }

    private void reloadCurrentSource(boolean preserveScroll) {
        if (currentSourceKey == null) return;
        Point scroll = preserveScroll ? captureReaderScrollPosition() : null;
        if (currentSourceKey.startsWith("BIBLE:")) showSelectedChapter(false);
        else if (currentSourceKey.startsWith("LIBRARY:")) showLibraryDoc(currentSourceKey.substring("LIBRARY:".length()));
        if (preserveScroll) restoreReaderScrollPosition(scroll);
    }

    private Point captureReaderScrollPosition() {
        JScrollPane sp = getReaderScrollPane();
        if (sp == null) return null;
        return sp.getViewport().getViewPosition();
    }

    private JScrollPane getReaderScrollPane() {
        return readerScrollPane;
    }

    private void restoreReaderScrollPosition(Point p) {
        if (p == null) return;
        SwingUtilities.invokeLater(() -> SwingUtilities.invokeLater(() -> {
            JScrollPane sp = getReaderScrollPane();
            if (sp == null) return;
            JViewport viewport = sp.getViewport();
            Dimension extent = viewport.getExtentSize();
            Dimension viewSize = viewport.getViewSize();
            int maxX = Math.max(0, viewSize.width - extent.width);
            int maxY = Math.max(0, viewSize.height - extent.height);
            viewport.setViewPosition(new Point(Math.max(0, Math.min(p.x, maxX)), Math.max(0, Math.min(p.y, maxY))));
        }));
    }

    private void showAnnotationDetails(java.util.List<TextAnnotation> annotations) {
        JPanel panel = ensureDetailsPanel();
        if (annotations == null || annotations.isEmpty()) return;
        annotations = new ArrayList<>(annotations);
        annotations.sort(this::compareAnnotationsForDisplay);
        if (annotations.size() == 1) {
            showAnnotationDetails(annotations.get(0));
            return;
        }

        panel.removeAll();
        TextAnnotation primary = primaryAnnotation(annotations);
        addDetailTitle(annotations.size() + " Attached Annotations");
        if (primary != null) {
            addDetailText("Source: " + primary.sourceTitle + "\nSelected text: “" + primary.selectedText + "”");
        }
        for (TextAnnotation a : annotations) {
            addDetailText(annotationDisplayName(a)
                    + (safe(a.category).isEmpty() ? "" : "\nCategory: " + a.category + "\nColor: " + colorHex(colorForCategory(a.category)))
                    + (safe(a.target).isEmpty() ? "" : "\nAttached to: " + a.target)
                    + (safe(a.note).isEmpty() ? "" : "\n" + a.note));
            JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
            actions.setOpaque(false);
            actions.setAlignmentX(Component.LEFT_ALIGNMENT);
            JButton edit = blackButton("Edit " + safe(a.type));
            edit.addActionListener(e -> editAnnotation(a));
            actions.add(edit);
            if (!safe(a.target).isEmpty()) {
                JButton open = blackButton("Open Target");
                open.addActionListener(e -> openAnnotationTarget(a));
                actions.add(open);
            }
            panel.add(actions);
            panel.add(Box.createVerticalStrut(8));
        }
        if (primary != null) {
            JButton pin = blackButton("Pin This Text To Sidebar");
            pin.setAlignmentX(Component.LEFT_ALIGNMENT);
            pin.addActionListener(e -> pinAnnotationToSidebar(primary));
            panel.add(pin);
        }
        panel.revalidate();
        panel.repaint();
    }

    private void showAnnotationDetails(TextAnnotation a) {
        JPanel panel = ensureDetailsPanel();
        panel.removeAll();
        addDetailTitle(a.type + " Highlight");
        addDetailText("Source: " + a.sourceTitle + "\nSelected text: “" + a.selectedText + "”"
                + "\nCreated: " + displayDate(a.createdAt) + "\nUpdated: " + displayDate(a.updatedAt));
        if (!a.category.isEmpty()) addDetailText("Category: " + a.category + "\nColor: " + colorHex(colorForCategory(a.category)));
        if (!a.target.isEmpty()) {
            addDetailText("Attached to: " + a.target);
            JButton open = blackButton("Open Attachment");
            open.setAlignmentX(Component.LEFT_ALIGNMENT);
            open.addActionListener(e -> openAnnotationTarget(a));
            panel.add(open);
            panel.add(Box.createVerticalStrut(8));
        }
        if (!a.note.isEmpty()) addDetailText(a.note);
        addLinkedReferencesSection(a);
        addRelatedTopicButtons("NOTE", a.id);

        JButton pin = blackButton("Pin This Highlight To Sidebar");
        pin.setAlignmentX(Component.LEFT_ALIGNMENT);
        pin.addActionListener(e -> pinAnnotationToSidebar(a));

        JButton addToProject = blackButton("Add To Study Project");
        addToProject.setAlignmentX(Component.LEFT_ALIGNMENT);
        addToProject.addActionListener(e -> addAnnotationToStudyProject(a));

        JButton edit = blackButton("Edit Highlight");
        edit.setAlignmentX(Component.LEFT_ALIGNMENT);
        edit.addActionListener(e -> editAnnotation(a));

        JButton del = blackButton("Delete Highlight");
        del.setAlignmentX(Component.LEFT_ALIGNMENT);
        del.addActionListener(e -> deleteAnnotation(a));

        panel.add(pin);
        panel.add(Box.createVerticalStrut(6));
        panel.add(addToProject);
        panel.add(Box.createVerticalStrut(6));
        panel.add(edit);
        panel.add(Box.createVerticalStrut(6));
        panel.add(del);
        panel.revalidate();
        panel.repaint();
    }


    private void refreshTopicPages() {
        if (topicPageModel == null || currentProfile == null) return;
        repairProfile(currentProfile);
        TopicPage selected = topicPageList == null ? null : topicPageList.getSelectedValue();
        String selectedId = selected == null ? "" : selected.id;
        topicPageModel.clear();
        String topicQuery = topicPageSearchField == null ? "" : topicPageSearchField.getText().trim().toLowerCase(Locale.ROOT);
        for (TopicPage topic : currentProfile.topicPages) {
            if (!topicQuery.isEmpty() && !(safe(topic.title) + " " + safe(topic.summary)).toLowerCase(Locale.ROOT).contains(topicQuery)) continue;
            topicPageModel.addElement(topic);
        }
        if (!selectedId.isEmpty()) selectTopicById(selectedId);
        else refreshSelectedTopicDetails();
    }

    private void refreshSelectedTopicDetails() {
        TopicPage topic = topicPageList == null ? null : topicPageList.getSelectedValue();
        if (topicTitleLabel != null) topicTitleLabel.setText(topic == null ? "No topic selected — create a topic to gather related verses, notes, and questions." : topic.title);
        if (topicSummaryArea != null) {
            topicSummaryArea.setEnabled(topic != null);
            topicSummaryArea.setText(topic == null ? "" : safe(topic.summary));
        }
        if (topicLinkModel != null) {
            topicLinkModel.clear();
            if (topic != null) {
                repairTopicPage(topic);
                for (LinkedItem link : topic.links) topicLinkModel.addElement(link);
            }
        }
    }

    private TopicPage selectedTopicPage() {
        return topicPageList == null ? null : topicPageList.getSelectedValue();
    }

    private TopicPage createTopicPage(String title) {
        TopicPage topic = new TopicPage(title);
        currentProfile.topicPages.add(topic);
        saveData();
        refreshTopicPages();
        selectTopicById(topic.id);
        return topic;
    }

    private void createTopicPageDialog() {
        String title = JOptionPane.showInputDialog(this, "Topic title:", "Create Topic", JOptionPane.PLAIN_MESSAGE);
        if (title == null || title.trim().isEmpty()) return;
        createTopicPage(title);
    }

    private void renameSelectedTopicPage() {
        TopicPage topic = selectedTopicPage();
        if (topic == null) return;
        String title = JOptionPane.showInputDialog(this, "New topic title:", topic.title);
        if (title == null || title.trim().isEmpty()) return;
        topic.title = title.trim();
        saveData();
        refreshTopicPages();
        selectTopicById(topic.id);
    }

    private void deleteSelectedTopicPage() {
        TopicPage topic = selectedTopicPage();
        if (topic == null) return;
        if (JOptionPane.showConfirmDialog(this, "Delete topic page: " + topic.title + "?", "Delete Topic", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
        currentProfile.topicPages.removeIf(t -> topic.id.equals(t.id));
        saveData();
        refreshTopicPages();
    }

    private void saveSelectedTopicSummary() {
        TopicPage topic = selectedTopicPage();
        if (topic == null) return;
        topic.summary = topicSummaryArea == null ? "" : topicSummaryArea.getText();
        saveData();
        statusLabel.setText(" Saved topic summary: " + topic.title);
    }

    private void selectTopicById(String id) {
        if (topicPageModel == null || topicPageList == null || id == null) return;
        for (int i = 0; i < topicPageModel.size(); i++) {
            TopicPage t = topicPageModel.get(i);
            if (id.equals(t.id)) {
                topicPageList.setSelectedIndex(i);
                topicPageList.ensureIndexIsVisible(i);
                refreshSelectedTopicDetails();
                return;
            }
        }
    }

    private void addCurrentVerseToSelectedTopic() {
        TopicPage topic = selectedTopicPage();
        if (topic == null) { JOptionPane.showMessageDialog(this, "Select or create a topic first."); return; }
        String ref = currentVerseReferenceFromSelectionOrCaret();
        if (ref.isEmpty()) ref = currentChapterReference();
        if (ref.isEmpty()) { JOptionPane.showMessageDialog(this, "Open a Bible chapter first."); return; }
        addLinkToTopic(topic, new LinkedItem("VERSE", ref, "related"));
    }

    private void addCurrentSelectionToSelectedTopic() {
        TopicPage topic = selectedTopicPage();
        if (topic == null) { JOptionPane.showMessageDialog(this, "Select or create a topic first."); return; }
        LinkedItem item = linkedItemForCurrentSelection("related");
        if (item == null) { JOptionPane.showMessageDialog(this, "Select text in the reader first."); return; }
        addLinkToTopic(topic, item);
    }

    private void addExistingNoteOrQuestionToSelectedTopic() {
        TopicPage topic = selectedTopicPage();
        if (topic == null) { JOptionPane.showMessageDialog(this, "Select or create a topic first."); return; }
        LinkedItem item = chooseExistingNoteOrQuestionLink();
        if (item != null) addLinkToTopic(topic, item);
    }

    private LinkedItem chooseExistingNoteOrQuestionLink() {
        DefaultListModel<LinkedItem> model = new DefaultListModel<>();
        for (TextAnnotation a : currentProfile.annotations) model.addElement(new LinkedItem("NOTE", a.id, shorten(sourceTitleFor(a) + " — " + a.selectedText, 80)));
        for (StudyQuestion q : currentProfile.questions) model.addElement(new LinkedItem("QUESTION", q.annotationId, shorten(q.sourceTitle + " — " + q.question, 80)));
        JList<LinkedItem> list = new JList<>(model);
        list.setVisibleRowCount(12);
        int result = JOptionPane.showConfirmDialog(this, new JScrollPane(list), "Choose Note or Question", JOptionPane.OK_CANCEL_OPTION);
        return result == JOptionPane.OK_OPTION ? list.getSelectedValue() : null;
    }

    private void removeSelectedTopicLink() {
        TopicPage topic = selectedTopicPage();
        LinkedItem item = topicLinkList == null ? null : topicLinkList.getSelectedValue();
        if (topic == null || item == null) return;
        topic.links.remove(item);
        saveData();
        refreshSelectedTopicDetails();
    }

    private void openSelectedTopicLink() {
        LinkedItem item = topicLinkList == null ? null : topicLinkList.getSelectedValue();
        if (item != null) openLinkedItem(item);
    }

    private void addSelectedTextToTopicPage() {
        LinkedItem item = linkedItemForCurrentSelection("related");
        if (item == null) { JOptionPane.showMessageDialog(this, "Select text in the reader first."); return; }
        addLinkedItemToTopicPage(item);
    }

    private void addAnnotationToTopicPage(TextAnnotation a) {
        if (a == null) return;
        addLinkedItemToTopicPage(new LinkedItem("NOTE", a.id, "related"));
    }

    private void addSelectedQuestionToTopicPage() {
        String s = questionList == null ? null : questionList.getSelectedValue();
        if (s == null) { JOptionPane.showMessageDialog(this, "Select a question first."); return; }
        try {
            int idx = Integer.parseInt(s.split("\\|")[0].trim());
            StudyQuestion q = currentProfile.questions.get(idx);
            addLinkedItemToTopicPage(new LinkedItem("QUESTION", q.annotationId, "related"));
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "Could not add selected question.");
        }
    }

    private void addLinkedItemToTopicPage(LinkedItem item) {
        if (item == null) return;
        TopicPage topic = chooseTopicPageWithCreateOption();
        if (topic == null) return;
        String label = JOptionPane.showInputDialog(this, "Relationship label:", safe(item.label).isEmpty() ? "related" : item.label);
        if (label == null) return;
        item.label = label.trim();
        addLinkToTopic(topic, item);
        selectTopicById(topic.id);
    }

    private TopicPage chooseTopicPageWithCreateOption() {
        repairProfile(currentProfile);
        String create = "Create New Topic…";
        DefaultComboBoxModel<Object> model = new DefaultComboBoxModel<>();
        model.addElement(create);
        for (TopicPage topic : currentProfile.topicPages) model.addElement(topic);
        JComboBox<Object> combo = new JComboBox<>(model);
        int result = JOptionPane.showConfirmDialog(this, combo, "Choose Topic Page", JOptionPane.OK_CANCEL_OPTION);
        if (result != JOptionPane.OK_OPTION) return null;
        Object selected = combo.getSelectedItem();
        if (selected instanceof TopicPage) return (TopicPage) selected;
        String title = JOptionPane.showInputDialog(this, "New topic title:", "New Topic", JOptionPane.PLAIN_MESSAGE);
        if (title == null || title.trim().isEmpty()) return null;
        return createTopicPage(title);
    }

    private void addLinkToTopic(TopicPage topic, LinkedItem item) {
        if (topic == null || item == null) return;
        repairTopicPage(topic);
        topic.links.add(item);
        saveData();
        refreshTopicPages();
        selectTopicById(topic.id);
        statusLabel.setText(" Added link to topic: " + topic.title);
    }

    private LinkedItem linkedItemForCurrentSelection(String label) {
        if (readerPane == null || currentSourceKey == null || currentSourceKey.isEmpty()) return null;
        boolean hasSelection = readerPane.getSelectionEnd() > readerPane.getSelectionStart();
        if (currentSourceKey.startsWith("BIBLE:")) {
            String ref = currentVerseReferenceFromSelectionOrCaret();
            if (ref.isEmpty()) ref = currentChapterReference();
            return ref.isEmpty() ? null : new LinkedItem("VERSE", ref, label);
        }
        if (currentSourceKey.startsWith("LIBRARY:")) {
            String selected = hasSelection ? safe(readerPane.getSelectedText()).trim() : "";
            String title = currentSourceKey.substring("LIBRARY:".length());
            String ref = "LIBRARY:" + title + (selected.isEmpty() ? "" : "::" + shorten(selected, 220));
            return new LinkedItem("BOOK", ref, label);
        }
        return null;
    }

    private String currentChapterReference() {
        if (currentSourceKey != null && currentSourceKey.startsWith("BIBLE:") && selectedBook != null && !selectedBook.isEmpty()) {
            return selectedBook + " " + selectedChapter;
        }
        return "";
    }

    private String currentVerseReferenceFromSelectionOrCaret() {
        if (currentSourceKey == null || !currentSourceKey.startsWith("BIBLE:")) return "";
        int pos = readerPane == null ? 0 : readerPane.getCaretPosition();
        if (readerPane != null && readerPane.getSelectionEnd() > readerPane.getSelectionStart()) pos = readerPane.getSelectionStart();
        Integer verse = verseNumberContainingPosition(pos);
        if (verse == null && readerPane != null && readerPane.getSelectionEnd() > readerPane.getSelectionStart()) verse = verseNumberContainingPosition(Math.max(readerPane.getSelectionStart(), readerPane.getSelectionEnd() - 1));
        return verse == null ? "" : selectedBook + " " + selectedChapter + ":" + verse;
    }

    private void createTopicFromCurrentSelection() {
        LinkedItem item = linkedItemForCurrentSelection("related");
        if (item == null) { JOptionPane.showMessageDialog(this, "Select text or open a verse first."); return; }
        createTopicFromLinkedItem(item);
    }

    private void createTopicFromLinkedItem(LinkedItem item) {
        String title = JOptionPane.showInputDialog(this, "Topic title:", "Create Topic From This", JOptionPane.PLAIN_MESSAGE);
        if (title == null || title.trim().isEmpty()) return;
        TopicPage topic = createTopicPage(title);
        topic.links.add(item);
        saveData();
        refreshTopicPages();
        selectTopicById(topic.id);
        showCard("topicPages");
    }

    private List<TopicPage> findTopicPagesLinkingTo(String type, String ref) {
        List<TopicPage> out = new ArrayList<>();
        if (currentProfile == null || type == null || ref == null) return out;
        repairProfile(currentProfile);
        String t = type.trim();
        String r = ref.trim();
        for (TopicPage topic : currentProfile.topicPages) {
            repairTopicPage(topic);
            for (LinkedItem link : topic.links) {
                if (link != null && t.equalsIgnoreCase(safe(link.type).trim()) && r.equalsIgnoreCase(safe(link.ref).trim())) {
                    out.add(topic);
                    break;
                }
            }
        }
        return out;
    }

    private void addRelatedTopicButtons(String type, String ref) {
        List<TopicPage> related = findTopicPagesLinkingTo(type, ref);
        if (related.isEmpty()) return;
        JPanel panel = ensureDetailsPanel();
        addDetailTitle("Related Topic Pages");
        for (TopicPage topic : related) {
            JButton b = blackButton(topic.title);
            b.setAlignmentX(Component.LEFT_ALIGNMENT);
            b.addActionListener(e -> { refreshTopicPages(); showCard("topicPages"); selectTopicById(topic.id); });
            panel.add(b);
            panel.add(Box.createVerticalStrut(6));
        }
    }

    private void addLinkedReferencesSection(TextAnnotation a) {
        repairAnnotation(a, System.currentTimeMillis());
        addDetailTitle("Linked References");
        if (a.links.isEmpty()) addDetailText("No linked references yet.");
        else for (LinkedItem link : a.links) addDetailText(link.toString());

        JButton add = blackButton("Add Link");
        JButton remove = blackButton("Remove Link");
        JButton open = blackButton("Open Link");
        JButton addTopic = blackButton("Add This Note to Topic Page");
        add.setAlignmentX(Component.LEFT_ALIGNMENT);
        remove.setAlignmentX(Component.LEFT_ALIGNMENT);
        open.setAlignmentX(Component.LEFT_ALIGNMENT);
        addTopic.setAlignmentX(Component.LEFT_ALIGNMENT);
        add.addActionListener(e -> addManualLinkToAnnotation(a));
        remove.addActionListener(e -> removeLinkFromAnnotation(a));
        open.addActionListener(e -> openLinkFromAnnotation(a));
        addTopic.addActionListener(e -> addAnnotationToTopicPage(a));
        JPanel panel = ensureDetailsPanel();
        panel.add(add); panel.add(Box.createVerticalStrut(6));
        panel.add(remove); panel.add(Box.createVerticalStrut(6));
        panel.add(open); panel.add(Box.createVerticalStrut(6));
        panel.add(addTopic); panel.add(Box.createVerticalStrut(8));
    }

    private void addManualLinkToAnnotation(TextAnnotation a) {
        String[] types = {"VERSE", "NOTE", "BOOK", "QUESTION", "GREEK", "TOPIC"};
        JComboBox<String> type = new JComboBox<>(types);
        JTextField ref = new JTextField(28);
        JTextField label = new JTextField("related", 28);
        JPanel p = new JPanel(new GridLayout(0, 1, 5, 5));
        p.add(new JLabel("Type:")); p.add(type);
        p.add(new JLabel("Reference:")); p.add(ref);
        p.add(new JLabel("Label:")); p.add(label);
        int result = JOptionPane.showConfirmDialog(this, p, "Add Linked Reference", JOptionPane.OK_CANCEL_OPTION);
        if (result != JOptionPane.OK_OPTION || ref.getText().trim().isEmpty()) return;
        a.links.add(new LinkedItem(String.valueOf(type.getSelectedItem()), ref.getText().trim(), label.getText().trim()));
        touchAnnotation(a);
        saveData();
        showAnnotationDetails(a);
    }

    private LinkedItem chooseAnnotationLink(TextAnnotation a) {
        if (a == null || a.links == null || a.links.isEmpty()) return null;
        JList<LinkedItem> list = new JList<>(new Vector<>(a.links));
        list.setVisibleRowCount(8);
        int result = JOptionPane.showConfirmDialog(this, new JScrollPane(list), "Choose Linked Reference", JOptionPane.OK_CANCEL_OPTION);
        return result == JOptionPane.OK_OPTION ? list.getSelectedValue() : null;
    }

    private void removeLinkFromAnnotation(TextAnnotation a) {
        LinkedItem link = chooseAnnotationLink(a);
        if (link == null) return;
        a.links.remove(link);
        touchAnnotation(a);
        saveData();
        showAnnotationDetails(a);
    }

    private void openLinkFromAnnotation(TextAnnotation a) {
        LinkedItem link = chooseAnnotationLink(a);
        if (link != null) openLinkedItem(link);
    }

    private void openLinkedItem(LinkedItem item) {
        if (item == null) return;
        String type = safe(item.type).trim().toUpperCase(Locale.ROOT);
        String ref = safe(item.ref).trim();
        if (ref.isEmpty()) return;
        if ("VERSE".equals(type)) {
            openTarget(ref);
            return;
        }
        if ("TOPIC".equals(type)) {
            refreshTopicPages();
            showCard("topicPages");
            selectTopicById(ref);
            return;
        }
        if ("NOTE".equals(type)) {
            TextAnnotation a = findAnnotationById(ref);
            if (a != null) { openSourceForAnnotation(a); safeSelect(a.start, a.end); showAnnotationDetails(a); showCard("study"); return; }
            showCard("recent");
            if (recentSearchField != null) { recentSearchField.setText(ref); refreshRecentNotes(); }
            return;
        }
        if ("QUESTION".equals(type)) {
            showCard("questions");
            refreshQuestions();
            return;
        }
        if ("CHAPTER_NOTE".equals(type)) {
            ChapterNote note = findChapterNoteById(ref);
            if (note != null) openChapterNoteViewer(note);
            return;
        }
        if ("BOOK".equals(type)) {
            openTarget(ref.startsWith("LIBRARY:") ? ref : "LIBRARY:" + ref);
            return;
        }
        if ("GREEK".equals(type)) {
            if (greekSearchField != null) {
                greekSearchField.setText(ref);
                doGreekSearch();
            }
            showCard("greekSearch");
        }
    }


    private void refreshMemoryVerses() {
        if (memoryModel == null || currentProfile == null) return;
        memoryModel.clear();
        refreshMemoryCategoryFilter();

        String q = memorySearchField == null ? "" : memorySearchField.getText().trim().toLowerCase(Locale.ROOT);
        String cat = memoryCategoryFilter == null || memoryCategoryFilter.getSelectedItem() == null
                ? "All Categories" : memoryCategoryFilter.getSelectedItem().toString();

        List<MemoryVerse> verses = new ArrayList<>(currentProfile.memoryVerses);
        verses.sort((a, b) -> Long.compare(b.createdAt, a.createdAt));
        for (MemoryVerse mv : verses) {
            repairMemoryVerse(mv);
            if (!"All Categories".equals(cat) && !cat.equals(safe(mv.category))) continue;
            if (!q.isEmpty() && !memoryVerseSearchText(mv).contains(q)) continue;
            memoryModel.addElement(mv);
        }
        if (memoryModel.isEmpty() && statusLabel != null && currentProfile.memoryVerses.isEmpty()) {
            statusLabel.setText(" Memory Verses is empty — select Bible text and choose Add To Memory Verses, or use Manual Add By Reference.");
        }
        updateHeader();
    }

    private void refreshMemoryCategoryFilter() {
        if (memoryCategoryFilter == null || currentProfile == null) return;
        String selected = memoryCategoryFilter.getSelectedItem() == null ? "All Categories" : memoryCategoryFilter.getSelectedItem().toString();
        Set<String> cats = new TreeSet<>();
        for (MemoryVerse mv : currentProfile.memoryVerses) {
            if (mv.category != null && !mv.category.trim().isEmpty()) cats.add(mv.category.trim());
        }
        ActionListener[] listeners = memoryCategoryFilter.getActionListeners();
        for (ActionListener l : listeners) memoryCategoryFilter.removeActionListener(l);
        memoryCategoryFilter.removeAllItems();
        memoryCategoryFilter.addItem("All Categories");
        for (String c : cats) memoryCategoryFilter.addItem(c);
        memoryCategoryFilter.setSelectedItem(cats.contains(selected) ? selected : "All Categories");
        for (ActionListener l : listeners) memoryCategoryFilter.addActionListener(l);
    }

    private String memoryVerseSearchText(MemoryVerse mv) {
        return (safe(mv.reference) + " " + safe(mv.text) + " " + safe(mv.category) + " " + safe(mv.note)).toLowerCase(Locale.ROOT);
    }

    private void addMemoryVerseFromSelection() {
        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        String selected = readerPane.getSelectedText();
        if (end <= start || selected == null || selected.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Select Bible text first, then choose Add To Memory Verses.");
            return;
        }

        String reference = currentSourceTitle;
        String text = selected.trim();
        if (currentSourceKey != null && currentSourceKey.startsWith("BIBLE:")) {
            Integer startVerse = verseNumberContainingPosition(start);
            Integer endVerse = verseNumberContainingPosition(Math.max(start, end - 1));
            if (startVerse == null) startVerse = endVerse;
            if (endVerse == null) endVerse = startVerse;
            if (startVerse != null) {
                int first = Math.min(startVerse, endVerse);
                int last = Math.max(startVerse, endVerse);
                PassageRef passage = new PassageRef(selectedBook, selectedChapter, first, last);
                reference = passage.display();
                String combined = getPassageText(passage.book, passage.chapter, passage.startVerse, passage.endVerse);
                if (!combined.isEmpty()) text = combined;
            }
        }
        showMemoryVerseEditor(null, reference, text);
    }

    private void addMemoryVerseByKey(String key) {
        Verse v = data.findVerse(key);
        if (v == null) {
            JOptionPane.showMessageDialog(this, "I could not find that Bible verse.");
            return;
        }
        showMemoryVerseEditor(null, v.key(), v.text);
    }

    private void addMemoryVerseManually() {
        String ref = JOptionPane.showInputDialog(this, "Bible reference or same-chapter range, e.g. Romans 14:13 or Genesis 3:1-7:");
        if (ref == null) return;
        PassageRef passage = parseBibleReferenceOrRange(ref);
        if (passage == null) {
            JOptionPane.showMessageDialog(this, "Use a format like Romans 14:13 or Genesis 3:1-7. Ranges must stay within one chapter.");
            return;
        }
        String text = getPassageText(passage.book, passage.chapter, passage.startVerse, passage.endVerse);
        if (text.isEmpty()) {
            JOptionPane.showMessageDialog(this, "I could not find that verse or complete passage range in the imported Bible text.");
            return;
        }
        showMemoryVerseEditor(null, passage.display(), text);
    }

    private PassageRef parseBibleReferenceOrRange(String input) {
        if (input == null) return null;
        String ref = input.trim().replaceAll("\\s+", " ");
        Matcher m = Pattern.compile("^(.+?)\\s+(\\d+)\\s*:\\s*(\\d+)(?:\\s*-\\s*(?:(\\d+)\\s*:\\s*)?(\\d+))?$").matcher(ref);
        if (!m.matches()) return null;
        try {
            String book = normalizeBookName(m.group(1));
            int chapter = Integer.parseInt(m.group(2));
            int startVerse = Integer.parseInt(m.group(3));
            if (m.group(4) != null) return null; // Cross-chapter ranges are intentionally not supported yet.
            int endVerse = m.group(5) == null ? startVerse : Integer.parseInt(m.group(5));
            if (startVerse <= 0 || endVerse < startVerse) return null;
            return new PassageRef(book, chapter, startVerse, endVerse);
        } catch (Exception e) {
            return null;
        }
    }

    private String getPassageText(String book, int chapter, int startVerse, int endVerse) {
        StringBuilder sb = new StringBuilder();
        Map<Integer, Verse> verses = data.getVerses(book, chapter);
        for (int verse = startVerse; verse <= endVerse; verse++) {
            Verse v = verses.get(verse);
            if (v == null) return "";
            if (sb.length() > 0) sb.append(" ");
            sb.append(v.verse).append(" ").append(v.text);
        }
        return sb.toString().trim();
    }

    private void showMemoryVerseEditor(MemoryVerse existing, String defaultReference, String defaultText) {
        JTextField reference = new JTextField(existing == null ? safe(defaultReference) : safe(existing.reference));
        JTextArea text = new JTextArea(existing == null ? safe(defaultText) : safe(existing.text), 5, 44);
        text.setLineWrap(true);
        text.setWrapStyleWord(true);
        JTextField category = new JTextField(existing == null ? "" : safe(existing.category));
        JTextArea note = new JTextArea(existing == null ? "" : safe(existing.note), 4, 44);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);

        JPanel p = new JPanel(new GridLayout(0, 1, 5, 5));
        p.add(new JLabel("Reference:"));
        p.add(reference);
        p.add(new JLabel("Verse text:"));
        p.add(new JScrollPane(text));
        p.add(new JLabel("Category:"));
        p.add(category);
        p.add(new JLabel("Note:"));
        p.add(new JScrollPane(note));

        int r = JOptionPane.showConfirmDialog(this, p, existing == null ? "Add Memory Verse" : "Edit Memory Verse", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        String ref = reference.getText().trim();
        String verseText = text.getText().trim();
        if (ref.isEmpty() || verseText.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Reference and verse text are required.");
            return;
        }

        MemoryVerse mv = existing == null ? new MemoryVerse() : existing;
        if (mv.id == null || mv.id.trim().isEmpty()) mv.id = UUID.randomUUID().toString();
        mv.reference = ref;
        mv.text = verseText;
        mv.category = category.getText().trim();
        mv.note = note.getText().trim();
        if (mv.createdAt <= 0L) mv.createdAt = System.currentTimeMillis();
        if (existing == null) currentProfile.memoryVerses.add(mv);

        saveData();
        refreshMemoryVerses();
        showCard("memory");
        log((existing == null ? "Added" : "Updated") + " memory verse: " + mv.reference);
    }

    private MemoryVerse selectedMemoryVerse() {
        return memoryList == null ? null : memoryList.getSelectedValue();
    }

    private void reviewSelectedMemoryVerse() {
        MemoryVerse mv = selectedMemoryVerse();
        if (mv == null) {
            JOptionPane.showMessageDialog(this, "Select a memory verse first.");
            return;
        }
        showMemoryReviewDialog(Collections.singletonList(mv), "Review Memory Verse");
    }

    private void editSelectedMemoryVerse() {
        MemoryVerse mv = selectedMemoryVerse();
        if (mv == null) {
            JOptionPane.showMessageDialog(this, "Select a memory verse first.");
            return;
        }
        showMemoryVerseEditor(mv, mv.reference, mv.text);
    }

    private void deleteSelectedMemoryVerse() {
        MemoryVerse mv = selectedMemoryVerse();
        if (mv == null) {
            JOptionPane.showMessageDialog(this, "Select a memory verse first.");
            return;
        }
        if (JOptionPane.showConfirmDialog(this, "Delete " + mv.reference + " from memory verses?", "Delete Memory Verse", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
        currentProfile.memoryVerses.removeIf(x -> safe(x.id).equals(safe(mv.id)));
        saveData();
        refreshMemoryVerses();
    }

    private void createMemoryFlashcards() {
        if (currentProfile == null || currentProfile.memoryVerses == null || currentProfile.memoryVerses.isEmpty()) {
            JOptionPane.showMessageDialog(this, "Add memory verses before starting flashcards.");
            return;
        }

        JRadioButton referenceToVerse = new JRadioButton("Reference → Verse", true);
        JRadioButton verseToReference = new JRadioButton("Verse → Reference");
        ButtonGroup modeGroup = new ButtonGroup();
        modeGroup.add(referenceToVerse);
        modeGroup.add(verseToReference);

        JCheckBox randomOrder = new JCheckBox("Random order");
        JCheckBox onlyMissedRecently = new JCheckBox("Only missed recently");
        JComboBox<String> categoryFilter = new JComboBox<>();
        categoryFilter.addItem("All Categories");
        for (String c : memoryVerseCategories()) categoryFilter.addItem(c);
        if (memoryCategoryFilter != null && memoryCategoryFilter.getSelectedItem() != null) {
            categoryFilter.setSelectedItem(memoryCategoryFilter.getSelectedItem().toString());
        }

        JPanel settings = new JPanel(new GridLayout(0, 1, 6, 6));
        settings.setBackground(panelBg);
        settings.add(new JLabel("Flashcard mode:"));
        settings.add(referenceToVerse);
        settings.add(verseToReference);
        settings.add(randomOrder);
        settings.add(onlyMissedRecently);
        settings.add(new JLabel("Category filter:"));
        settings.add(categoryFilter);

        int r = JOptionPane.showConfirmDialog(this, settings, "Flashcard Settings", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;

        List<MemoryVerse> cards = filteredMemoryFlashcards(categoryFilter.getSelectedItem(), onlyMissedRecently.isSelected());
        if (cards.isEmpty()) {
            JOptionPane.showMessageDialog(this, "No memory verses match those flashcard settings.");
            return;
        }
        if (randomOrder.isSelected()) Collections.shuffle(cards);
        showMemoryReviewDialog(cards, "Memory Verse Flashcards", verseToReference.isSelected());
    }

    private Set<String> memoryVerseCategories() {
        Set<String> cats = new TreeSet<>();
        if (currentProfile == null || currentProfile.memoryVerses == null) return cats;
        for (MemoryVerse mv : currentProfile.memoryVerses) {
            if (mv == null) continue;
            String c = safe(mv.category).trim();
            if (!c.isEmpty()) cats.add(c);
        }
        return cats;
    }

    private List<MemoryVerse> filteredMemoryFlashcards(Object selectedCategory, boolean onlyMissedRecently) {
        String category = selectedCategory == null ? "All Categories" : selectedCategory.toString();
        List<MemoryVerse> cards = new ArrayList<>();
        if (currentProfile == null || currentProfile.memoryVerses == null) return cards;
        for (MemoryVerse mv : currentProfile.memoryVerses) {
            repairMemoryVerse(mv);
            if (!"All Categories".equals(category) && !category.equals(safe(mv.category))) continue;
            if (onlyMissedRecently && !wasMissedRecently(mv)) continue;
            cards.add(mv);
        }
        cards.sort((a, b) -> Long.compare(b.createdAt, a.createdAt));
        return cards;
    }

    private boolean wasMissedRecently(MemoryVerse mv) {
        long thirtyDaysAgo = System.currentTimeMillis() - 30L * 24L * 60L * 60L * 1000L;
        return mv != null && mv.lastReviewedAt >= thirtyDaysAgo && mv.correctCount < mv.reviewCount;
    }

    private void showMemoryReviewDialog(List<MemoryVerse> verses, String title) {
        showMemoryReviewDialog(verses, title, false);
    }

    private void showMemoryReviewDialog(List<MemoryVerse> verses, String title, boolean verseToReference) {
        if (verses == null || verses.isEmpty()) return;
        final int[] index = {0};
        final int[] reviewed = {0};
        final int[] correct = {0};
        final boolean[] showingAnswer = {false};

        JDialog dialog = new JDialog(this, title, true);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.setLayout(new BorderLayout(8, 8));
        dialog.getContentPane().setBackground(panelBg);

        JLabel progress = new JLabel(" ");
        progress.setBorder(new EmptyBorder(10, 12, 0, 12));
        progress.setForeground(darkRed);
        progress.setFont(new Font("Segoe UI", Font.BOLD, 16));

        JTextArea cardText = readonlyArea();
        cardText.setFont(new Font("Georgia", Font.PLAIN, 20));

        JButton showAnswer = blackButton("Show Answer");
        JButton gotIt = blackButton("Got It");
        JButton missed = blackButton("Missed It");
        JButton next = blackButton("Next");
        JButton exit = blackButton("Exit");
        gotIt.setVisible(false);
        missed.setVisible(false);

        Runnable render = () -> {
            MemoryVerse mv = verses.get(index[0]);
            String frontLabel = verseToReference ? "Verse Text" : "Reference";
            String backLabel = verseToReference ? "Reference" : "Verse Text";
            String front = verseToReference ? safe(mv.text) : safe(mv.reference);
            String back = verseToReference ? safe(mv.reference) : safe(mv.text);
            progress.setText("Session: " + correct[0] + " correct / " + reviewed[0] + " reviewed   •   Card "
                    + (index[0] + 1) + " of " + verses.size());
            cardText.setText(showingAnswer[0]
                    ? frontLabel + "\n" + front + "\n\n" + backLabel + "\n" + back
                            + (safe(mv.note).isEmpty() ? "" : "\n\nNote: " + mv.note)
                    : frontLabel + "\n" + front + "\n\n(Click Show Answer when ready.)");
            cardText.setCaretPosition(0);
            showAnswer.setVisible(!showingAnswer[0]);
            gotIt.setVisible(showingAnswer[0]);
            missed.setVisible(showingAnswer[0]);
        };

        Runnable advance = () -> {
            if (index[0] + 1 >= verses.size()) {
                saveData();
                refreshMemoryVerses();
                dialog.dispose();
                JOptionPane.showMessageDialog(this, "Flashcard review complete. Session: "
                        + correct[0] + " correct / " + reviewed[0] + " reviewed.");
                return;
            }
            index[0]++;
            showingAnswer[0] = false;
            render.run();
        };

        showAnswer.addActionListener(e -> {
            showingAnswer[0] = true;
            render.run();
        });
        gotIt.addActionListener(e -> {
            reviewed[0]++;
            correct[0]++;
            markMemoryVerseReviewed(verses.get(index[0]), true);
            advance.run();
        });
        missed.addActionListener(e -> {
            reviewed[0]++;
            markMemoryVerseReviewed(verses.get(index[0]), false);
            advance.run();
        });
        next.addActionListener(e -> advance.run());
        exit.addActionListener(e -> {
            saveData();
            refreshMemoryVerses();
            dialog.dispose();
        });

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 8));
        buttons.setBackground(panelBg);
        buttons.add(showAnswer);
        buttons.add(gotIt);
        buttons.add(missed);
        buttons.add(next);
        buttons.add(exit);

        dialog.add(progress, BorderLayout.NORTH);
        dialog.add(new JScrollPane(cardText), BorderLayout.CENTER);
        dialog.add(buttons, BorderLayout.SOUTH);
        applyModernTheme(dialog);
        render.run();
        dialog.setMinimumSize(new Dimension(600, 390));
        dialog.setSize(680, 460);
        dialog.setLocationRelativeTo(this);
        dialog.setVisible(true);
    }

    private void markMemoryVerseReviewed(MemoryVerse mv, boolean correct) {
        if (mv == null) return;
        mv.reviewCount++;
        if (correct) mv.correctCount++;
        mv.lastReviewedAt = System.currentTimeMillis();
        saveData();
    }

    private void togglePinnedItems() {
        pinnedItemsExpanded = !pinnedItemsExpanded;
        if (pinnedItemsScroll != null) pinnedItemsScroll.setVisible(pinnedItemsExpanded);
        if (pinnedItemsToggleBtn != null) pinnedItemsToggleBtn.setText(pinnedItemsExpanded ? "Minimize" : "Expand");
        if (pinnedItemsPanel != null) {
            pinnedItemsPanel.revalidate();
            pinnedItemsPanel.repaint();
        }
    }

    private void refreshPinnedItems() {
        if (pinnedItemsBody == null) return;
        pinnedItemsBody.removeAll();

        if (currentProfile == null || currentProfile.pinnedItems == null || currentProfile.pinnedItems.isEmpty()) {
            JLabel empty = new JLabel("No pinned study items yet.");
            empty.setForeground(new Color(100, 70, 55));
            empty.setAlignmentX(Component.LEFT_ALIGNMENT);
            pinnedItemsBody.add(empty);
        } else {
            List<PinnedItem> items = new ArrayList<>(currentProfile.pinnedItems);
            items.sort((a, b) -> Long.compare(b.createdAt, a.createdAt));
            for (PinnedItem item : items) addPinnedItemCard(item);
        }

        pinnedItemsBody.revalidate();
        pinnedItemsBody.repaint();
    }

    private void addPinnedItemCard(PinnedItem item) {
        JPanel card = new JPanel(new BorderLayout(5, 5));
        card.setBackground(new Color(255, 253, 248));
        card.setBorder(new CompoundBorder(new LineBorder(new Color(210, 185, 160)), new EmptyBorder(6, 6, 6, 6)));
        card.setAlignmentX(Component.LEFT_ALIGNMENT);

        String title = item.sourceTitle == null || item.sourceTitle.trim().isEmpty() ? safe(item.sourceKey) : item.sourceTitle;
        JLabel header = new JLabel("<html><b>" + esc(title) + "</b></html>");
        header.setForeground(darkRed);

        String noteLine = pinnedItemNoteLine(item);
        JLabel body = new JLabel("<html>“" + esc(shorten(item.selectedText, 120)) + "”"
                + (noteLine.isEmpty() ? "" : "<br><span style='color:#5f4035;'>" + esc(noteLine) + "</span>")
                + "</html>");
        body.setFont(new Font("Segoe UI", Font.PLAIN, 12));

        JPanel buttons = new JPanel(new GridLayout(0, 1, 5, 5));
        buttons.setOpaque(false);
        JButton open = blackButton("Open");
        open.addActionListener(e -> openPinnedItem(item));
        JButton addToProject = blackButton("Add Pin To Study Project");
        addToProject.addActionListener(e -> addPinnedItemToStudyProject(item));
        JButton remove = blackButton("Remove pin");
        remove.addActionListener(e -> removePinnedItem(item));
        buttons.add(open);
        buttons.add(addToProject);
        buttons.add(remove);

        card.add(header, BorderLayout.NORTH);
        card.add(body, BorderLayout.CENTER);
        card.add(buttons, BorderLayout.SOUTH);
        pinnedItemsBody.add(card);
        pinnedItemsBody.add(Box.createVerticalStrut(6));
    }

    private String pinnedItemNoteLine(PinnedItem item) {
        TextAnnotation a = item.annotationId == null || item.annotationId.trim().isEmpty() ? null : findAnnotationById(item.annotationId);
        String type = a == null ? "" : a.type;
        String note = item.note == null ? "" : item.note.trim();
        if (!type.isEmpty() && !note.isEmpty()) return type + ": " + shorten(note, 90);
        if (!type.isEmpty()) return type;
        return shorten(note, 90);
    }

    private void pinSelectedTextToSidebar() {
        if (readerPane == null || currentProfile == null) return;
        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        if (end <= start || readerPane.getSelectedText() == null || readerPane.getSelectedText().trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Select text first, then choose Pin Selected Text To Sidebar.");
            return;
        }

        JTextArea note = new JTextArea(4, 36);
        note.setLineWrap(true);
        note.setWrapStyleWord(true);
        int choice = JOptionPane.showConfirmDialog(this, new JScrollPane(note), "Optional pin note", JOptionPane.OK_CANCEL_OPTION);
        if (choice != JOptionPane.OK_OPTION) return;

        PinnedItem item = createPinnedItem(currentSourceKey, currentSourceTitle, start, end, readerPane.getSelectedText(), note.getText().trim(), "");
        currentProfile.pinnedItems.add(item);
        saveData();
        refreshPinnedItems();
        log("Pinned selected text to sidebar.");
    }

    private void pinAnnotationToSidebar(TextAnnotation a) {
        if (a == null || currentProfile == null) return;
        for (PinnedItem item : currentProfile.pinnedItems) {
            if (a.id != null && a.id.equals(item.annotationId)) {
                JOptionPane.showMessageDialog(this, "This highlight is already pinned.");
                return;
            }
        }
        PinnedItem item = createPinnedItem(a.sourceKey, a.sourceTitle, a.start, a.end, a.selectedText, a.note, a.id);
        currentProfile.pinnedItems.add(item);
        saveData();
        refreshPinnedItems();
        log("Pinned highlight to sidebar.");
    }

    private PinnedItem createPinnedItem(String sourceKey, String sourceTitle, int start, int end, String selectedText, String note, String annotationId) {
        PinnedItem item = new PinnedItem();
        item.id = UUID.randomUUID().toString();
        item.sourceKey = sourceKey == null ? "" : sourceKey;
        item.sourceTitle = sourceTitle == null ? "" : sourceTitle;
        item.start = start;
        item.end = end;
        item.selectedText = selectedText == null ? "" : selectedText;
        item.note = note == null ? "" : note;
        item.annotationId = annotationId == null ? "" : annotationId;
        item.createdAt = System.currentTimeMillis();
        return item;
    }

    private void removePinnedItem(PinnedItem item) {
        if (item == null || currentProfile == null || currentProfile.pinnedItems == null) return;
        currentProfile.pinnedItems.removeIf(p -> safe(p.id).equals(safe(item.id)));
        saveData();
        refreshPinnedItems();
    }

    private void openPinnedItem(PinnedItem item) {
        if (item == null) return;
        TextAnnotation a = item.annotationId == null || item.annotationId.trim().isEmpty() ? null : findAnnotationById(item.annotationId);
        if (a != null) {
            openSourceForAnnotation(a);
            safeSelect(a.start, a.end);
            showAnnotationDetails(a);
        } else {
            openSourceForPinnedItem(item);
            safeSelect(item.start, item.end);
            showSourceSummary(item.sourceKey, item.sourceTitle);
        }
        showCard("study");
    }

    private void openSourceForPinnedItem(PinnedItem item) {
        if (item == null || item.sourceKey == null) return;
        if (item.sourceKey.startsWith("BIBLE:")) {
            String ref = item.sourceKey.substring("BIBLE:".length()) + ":1";
            RefParts rp = parseRef(ref);
            if (rp != null) {
                selectedBook = rp.book;
                selectedChapter = rp.chapter;
                refreshBookCombo();
                showSelectedChapter(false);
            }
        } else if (item.sourceKey.startsWith("LIBRARY:")) {
            showLibraryDoc(item.sourceKey.substring("LIBRARY:".length()));
        }
    }

    private void openAnnotationTarget(TextAnnotation a) {
        if (a == null || a.target == null || a.target.trim().isEmpty()) return;
        openTarget(a.target.trim());
    }

    private void openTarget(String target) {
        if (target == null || target.trim().isEmpty()) return;
        target = target.trim();

        if (target.startsWith("LIBRARY:")) {
            String rest = target.substring("LIBRARY:".length());
            String title = rest;
            String chunk = "";
            int split = rest.indexOf("::");
            if (split >= 0) {
                title = rest.substring(0, split);
                chunk = rest.substring(split + 2);
            }
            showLibraryDoc(title);
            if (!chunk.isEmpty()) findAndSelectUniqueChunk(chunk);
            showCard("study");
            return;
        }

        ChapterRef chapterRef = parseChapterRef(target);
        if (chapterRef != null && data.bible.containsKey(chapterRef.book)) {
            selectedBook = chapterRef.book;
            selectedChapter = chapterRef.chapter;
            refreshBookCombo();
            showSelectedChapter(false);
            showCard("study");
            return;
        }

        PassageRef passage = parseBibleReferenceOrRange(target);
        if (passage != null && data.bible.containsKey(passage.book)) {
            selectedBook = passage.book;
            selectedChapter = passage.chapter;
            refreshBookCombo();
            showSelectedChapter(false);
            selectVerseText(passage.startVerse);
            showCard("study");
            return;
        }

        RefParts rp = parseRef(target);
        if (rp != null && data.bible.containsKey(rp.book)) {
            selectedBook = rp.book;
            selectedChapter = rp.chapter;
            refreshBookCombo();
            showSelectedChapter(false);
            selectVerseText(rp.verse);
            showCard("study");
        }
    }

    private void findAndSelectUniqueChunk(String chunk) {
        try {
            String text = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            String nText = normalizeForFind(text);
            String nChunk = normalizeForFind(chunk);
            int idx = nText.indexOf(nChunk);
            if (idx < 0) return;
            int second = nText.indexOf(nChunk, idx + 1);
            if (second >= 0) return;

            int[] real = normalizedIndexToRealRange(text, chunk, idx);
            readerPane.requestFocusInWindow();
            readerPane.select(real[0], real[1]);
            readerPane.setCaretPosition(real[0]);
        } catch (Exception ignored) {}
    }

    private int[] normalizedIndexToRealRange(String full, String chunk, int normalizedStart) {
        StringBuilder norm = new StringBuilder();
        int realStart = -1;
        int realEnd = full.length();
        int targetLen = normalizeForFind(chunk).length();

        for (int i = 0; i < full.length(); i++) {
            char c = full.charAt(i);
            boolean add = !Character.isWhitespace(c) || (norm.length() > 0 && norm.charAt(norm.length() - 1) != ' ');
            if (add) {
                if (norm.length() == normalizedStart) realStart = i;
                norm.append(Character.isWhitespace(c) ? ' ' : Character.toLowerCase(c));
                if (realStart >= 0 && norm.length() >= normalizedStart + targetLen) {
                    realEnd = i + 1;
                    break;
                }
            }
        }
        if (realStart < 0) realStart = 0;
        return new int[]{realStart, realEnd};
    }

    private void selectVerseText(int verse) {
        try {
            String txt = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            String marker = "\n" + verse + " ";
            int idx = txt.indexOf(marker);
            if (idx < 0 && txt.startsWith(verse + " ")) idx = 0;
            if (idx >= 0) {
                int start = idx + (idx == 0 ? 0 : 1);
                int end = txt.indexOf("\n\n", start);
                if (end < 0) end = txt.length();
                readerPane.requestFocusInWindow();
                readerPane.select(start, end);
                readerPane.setCaretPosition(start);
            }
        } catch (Exception ignored) {}
    }

    private void showSourceSummary(String sourceKey, String sourceTitle) {
        JPanel panel = ensureDetailsPanel();
        panel.removeAll();
        addDetailTitle(sourceTitle == null || sourceTitle.isEmpty() ? "Current Source" : sourceTitle);
        int count = 0;
        for (TextAnnotation a : currentProfile.annotations) if (sourceKey.equals(a.sourceKey)) count++;
        addDetailText(count + " highlight note(s) in this source.\n\nSelect text and right-click to add a note, category, attachment, question, or topic link. Hover over highlighted text to preview notes. Click a highlight to view actions here.");
        if (sourceKey != null && sourceKey.startsWith("BIBLE:")) addRelatedTopicButtons("VERSE", sourceKey.substring("BIBLE:".length()));
        panel.revalidate();
        panel.repaint();
    }

    private void showDetailsText(String text) {
        JPanel panel = ensureDetailsPanel();
        panel.removeAll();

        if (text != null && !text.trim().isEmpty()) {
            addDetailText(text);
        }

        panel.revalidate();
        panel.repaint();
    }

    private void addDetailTitle(String s) {
        JLabel l = new JLabel("<html><b>" + esc(s) + "</b></html>");
        l.setFont(new Font("Segoe UI", Font.BOLD, 18));
        l.setForeground(darkRed);
        l.setAlignmentX(Component.LEFT_ALIGNMENT);
        JPanel panel = ensureDetailsPanel();
        panel.add(l);
        panel.add(Box.createVerticalStrut(8));
    }

    private void addDetailText(String s) {
        JTextArea a = readonlyArea();
        a.setText(s == null ? "" : s);
        a.setAlignmentX(Component.LEFT_ALIGNMENT);
        JPanel panel = ensureDetailsPanel();
        panel.add(a);
        panel.add(Box.createVerticalStrut(8));
    }

    private String createCategory(String optionalAnnotationId) {
        JTextField name = new JTextField();
        JTextArea desc = new JTextArea(5, 35);
        desc.setLineWrap(true);
        desc.setWrapStyleWord(true);

        JButton chooseColor = blackButton("Choose Highlight Color");
        JLabel colorPreview = new JLabel("Default blue " + colorHex(categoryBlue));
        colorPreview.setOpaque(true);
        colorPreview.setBackground(categoryBlue);
        colorPreview.setBorder(new CompoundBorder(new LineBorder(Color.DARK_GRAY), new EmptyBorder(6, 8, 6, 8)));

        final Color[] selectedColor = new Color[]{categoryBlue};
        chooseColor.addActionListener(e -> {
            Color c = JColorChooser.showDialog(this, "Choose Category Highlight Color", selectedColor[0]);
            if (c != null) {
                selectedColor[0] = c;
                colorPreview.setBackground(c);
                colorPreview.setText(colorHex(c));
            }
        });

        JPanel colorPanel = new JPanel(new BorderLayout(6, 6));
        colorPanel.add(chooseColor, BorderLayout.WEST);
        colorPanel.add(colorPreview, BorderLayout.CENTER);

        JPanel p = new JPanel(new BorderLayout(6, 6));
        p.add(new JLabel("Category name:"), BorderLayout.NORTH);
        p.add(name, BorderLayout.CENTER);

        JPanel bottom = new JPanel(new BorderLayout(6, 6));
        bottom.add(new JScrollPane(desc), BorderLayout.CENTER);
        bottom.add(colorPanel, BorderLayout.SOUTH);
        p.add(bottom, BorderLayout.SOUTH);

        int r = JOptionPane.showConfirmDialog(this, p, "Create Category", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return null;

        String n = name.getText().trim();
        if (n.isEmpty()) return null;

        currentProfile.categories.put(n, desc.getText().trim());
        ensureCategoryColors();
        currentProfile.categoryColors.put(n, selectedColor[0].getRGB());

        saveData();
        refreshCategories();
        return n;
    }

    private void refreshCategories() {
        if (categoryModel == null) return;
        ensureCategoryColors();
        String selected = selectedCategoryNameFromListValue(categoryList == null ? null : categoryList.getSelectedValue());
        categoryModel.clear();

        String categoryQuery = categorySearchField == null ? "" : categorySearchField.getText().trim().toLowerCase(Locale.ROOT);
        int selectedIndex = -1;
        for (String c : currentProfile.categories.keySet()) {
            if (!categoryQuery.isEmpty() && !(c + " " + currentProfile.categories.getOrDefault(c, "")).toLowerCase(Locale.ROOT).contains(categoryQuery)) continue;
            currentProfile.categoryColors.putIfAbsent(c, categoryBlue.getRGB());
            int count = 0;
            for (TextAnnotation a : currentProfile.annotations) if (c.equals(a.category)) count++;
            String row = c + " (" + count + ") " + colorHex(colorForCategory(c));
            categoryModel.addElement(row);
            if (!selected.isEmpty() && selected.equals(c)) selectedIndex = categoryModel.size() - 1;
        }
        if (selectedIndex >= 0) {
            categoryList.setSelectedIndex(selectedIndex);
            categoryList.ensureIndexIsVisible(selectedIndex);
        } else if (categoryModel.size() > 0 && (selected.isEmpty() || categoryList.getSelectedIndex() < 0)) {
            categoryList.setSelectedIndex(0);
        } else if (categoryModel.isEmpty()) {
            renderSelectedCategoryResults();
        }
    }

    private void ensureCategoryColors() {
        if (currentProfile.categoryColors == null) currentProfile.categoryColors = new TreeMap<>();
        if (currentProfile.categories == null) currentProfile.categories = new TreeMap<>();
        for (String c : currentProfile.categories.keySet()) currentProfile.categoryColors.putIfAbsent(c, categoryBlue.getRGB());
    }

    private void changeSelectedCategoryColor() {
        String s = categoryList.getSelectedValue();
        if (s == null) {
            JOptionPane.showMessageDialog(this, "Select a category first.");
            return;
        }

        String cat = selectedCategoryNameFromListValue(s);
        changeCategoryColorByName(cat);
    }

    private void changeCategoryColorByName(String cat) {
        if (cat == null) return;
        cat = cat.trim();
        if (cat.isEmpty()) return;

        Color current = colorForCategory(cat);
        Color chosen = JColorChooser.showDialog(this, "Choose Highlight Color for " + cat, current);
        if (chosen == null) return;

        ensureCategoryColors();
        currentProfile.categoryColors.put(cat, chosen.getRGB());
        saveData();
        refreshCategories();
        renderSelectedCategoryResults();
        reloadCurrentSource();
        showCategoryDetails(cat);
        JOptionPane.showMessageDialog(this, "Updated highlight color for: " + cat);
    }

    private String selectedCategoryNameFromListValue(String s) {
        if (s == null) return "";
        return s.replaceAll("\\s+\\(.*$", "").trim();
    }

    private void renderSelectedCategoryResults() {
        if (categoryResultsPanel == null) return;
        categoryResultsPanel.removeAll();

        String cat = selectedCategoryNameFromListValue(categoryList == null ? null : categoryList.getSelectedValue());
        if (cat.isEmpty()) {
            if (selectedCategoryTitleLabel != null) selectedCategoryTitleLabel.setText("Select a category");
            addCategoryResultsEmptyMessage("Choose a category on the left to browse its attached verses and notes.");
            refreshCategoryResultsPanel();
            return;
        }

        List<TextAnnotation> entries = getEntriesForCategory(cat);
        if (selectedCategoryTitleLabel != null) {
            selectedCategoryTitleLabel.setText(cat + " — " + entries.size() + " item" + (entries.size() == 1 ? "" : "s"));
        }

        String q = categoryResultSearchField == null ? "" : categoryResultSearchField.getText().trim().toLowerCase(Locale.ROOT);
        int shown = 0;
        for (TextAnnotation a : entries) {
            if (!q.isEmpty() && !categoryResultSearchText(a).contains(q)) continue;
            categoryResultsPanel.add(buildCategoryResultCard(a, expandedCategoryResultIds.contains(safe(a.id))));
            categoryResultsPanel.add(Box.createVerticalStrut(8));
            shown++;
        }

        if (shown == 0) {
            addCategoryResultsEmptyMessage(q.isEmpty()
                    ? "This category does not have any attached highlights yet."
                    : "No attached items match your search in this category.");
        }

        refreshCategoryResultsPanel();
    }

    private List<TextAnnotation> getEntriesForCategory(String category) {
        List<TextAnnotation> entries = new ArrayList<>();
        if (currentProfile == null || currentProfile.annotations == null || category == null) return entries;
        for (TextAnnotation a : currentProfile.annotations) {
            if (a != null && category.equals(safe(a.category).trim())) entries.add(a);
        }
        entries.sort((a, b) -> Long.compare(b.createdAt, a.createdAt));
        return entries;
    }

    private JPanel buildCategoryResultCard(TextAnnotation a, boolean expanded) {
        JPanel card = new JPanel(new BorderLayout(8, 8));
        card.setBackground(modernSurface);
        card.setBorder(new CompoundBorder(new LineBorder(colorForCategory(a.category), 2, true), new EmptyBorder(10, 12, 10, 12)));
        card.setAlignmentX(Component.LEFT_ALIGNMENT);
        card.setMaximumSize(new Dimension(Integer.MAX_VALUE, expanded ? 360 : 150));
        card.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

        String location = categoryResultLocation(a);
        JLabel header = new JLabel("<html><b>" + esc(location) + "</b> — " + esc(a.wholeChapter ? "Whole chapter" : shorten(a.selectedText, 140)) + "</html>");
        header.setForeground(darkRed);
        header.setFont(new Font("Segoe UI", Font.BOLD, 14));

        JTextArea body = readonlyArea();
        body.setBackground(modernSurface);
        body.setBorder(new EmptyBorder(2, 0, 2, 0));
        body.setText(expanded ? categoryResultExpandedText(a) : categoryResultCollapsedText(a));
        body.setRows(expanded ? 7 : 2);
        body.setCaretPosition(0);
        body.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));

        JButton expand = blackButton(expanded ? "Collapse" : "Expand");
        expand.addActionListener(e -> toggleCategoryResultExpanded(a));
        JButton jump = blackButton("Jump To");
        jump.addActionListener(e -> jumpToCategoryResult(a));

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
        buttons.setOpaque(false);
        buttons.add(expand);
        buttons.add(jump);

        MouseAdapter toggle = new MouseAdapter() {
            public void mouseClicked(MouseEvent e) {
                if (SwingUtilities.isLeftMouseButton(e) && e.getClickCount() == 1) toggleCategoryResultExpanded(a);
            }
        };
        card.addMouseListener(toggle);
        header.addMouseListener(toggle);
        body.addMouseListener(toggle);

        card.add(header, BorderLayout.NORTH);
        card.add(body, BorderLayout.CENTER);
        card.add(buttons, BorderLayout.SOUTH);
        return card;
    }

    private String categoryResultLocation(TextAnnotation a) {
        String title = safe(a.sourceTitle).trim();
        if (!title.isEmpty()) return title;
        String key = safe(a.sourceKey).trim();
        if (key.startsWith("BIBLE:")) return key.substring("BIBLE:".length());
        if (key.startsWith("LIBRARY:")) return key.substring("LIBRARY:".length());
        return key.isEmpty() ? "Unknown source" : key;
    }

    private String categoryResultCollapsedText(TextAnnotation a) {
        StringBuilder sb = new StringBuilder();
        sb.append(a.wholeChapter ? "Whole chapter: " + categoryResultLocation(a) : shorten(a.selectedText, 220));
        if (!safe(a.note).trim().isEmpty()) sb.append("\nNote: ").append(shorten(a.note, 160));
        if (!safe(a.category).trim().isEmpty()) sb.append("\nCategory: ").append(a.category);
        return sb.toString();
    }

    private String categoryResultExpandedText(TextAnnotation a) {
        StringBuilder sb = new StringBuilder();
        sb.append(a.wholeChapter ? "Whole chapter: " + categoryResultLocation(a) : safe(a.selectedText).trim());
        if (!safe(a.note).trim().isEmpty()) sb.append("\n\nNote / comment:\n").append(a.note.trim());
        if (!safe(a.category).trim().isEmpty()) sb.append("\n\nCategory: ").append(a.category.trim());
        if (!safe(a.target).trim().isEmpty()) sb.append("\nTarget: ").append(a.target.trim());
        return sb.toString();
    }

    private String categoryResultSearchText(TextAnnotation a) {
        return (categoryResultLocation(a) + " " + safe(a.selectedText) + " " + safe(a.note) + " "
                + safe(a.category) + " " + safe(a.target)).toLowerCase(Locale.ROOT);
    }

    private void toggleCategoryResultExpanded(TextAnnotation a) {
        if (a == null || a.id == null) return;
        if (expandedCategoryResultIds.contains(a.id)) expandedCategoryResultIds.remove(a.id);
        else expandedCategoryResultIds.add(a.id);
        renderSelectedCategoryResults();
    }

    private void expandAllCategoryResults() {
        String cat = selectedCategoryNameFromListValue(categoryList == null ? null : categoryList.getSelectedValue());
        if (cat.isEmpty()) return;
        String q = categoryResultSearchField == null ? "" : categoryResultSearchField.getText().trim().toLowerCase(Locale.ROOT);
        for (TextAnnotation a : getEntriesForCategory(cat)) {
            if (q.isEmpty() || categoryResultSearchText(a).contains(q)) expandedCategoryResultIds.add(a.id);
        }
        renderSelectedCategoryResults();
    }

    private void collapseAllCategoryResults() {
        String cat = selectedCategoryNameFromListValue(categoryList == null ? null : categoryList.getSelectedValue());
        if (cat.isEmpty()) return;
        String q = categoryResultSearchField == null ? "" : categoryResultSearchField.getText().trim().toLowerCase(Locale.ROOT);
        for (TextAnnotation a : getEntriesForCategory(cat)) {
            if (q.isEmpty() || categoryResultSearchText(a).contains(q)) expandedCategoryResultIds.remove(a.id);
        }
        renderSelectedCategoryResults();
    }

    private void jumpToCategoryResult(TextAnnotation a) {
        if (a == null) return;
        openSourceForAnnotation(a);
        if (a.wholeChapter) moveReaderCaret(0); else safeSelect(a.start, a.end);
        showAnnotationDetails(a);
        showCard("study");
    }

    private void addCategoryResultsEmptyMessage(String message) {
        JLabel empty = new JLabel("<html>" + esc(message) + "</html>");
        empty.setForeground(modernMutedText);
        empty.setFont(new Font("Segoe UI", Font.PLAIN, 15));
        empty.setBorder(new EmptyBorder(16, 16, 16, 16));
        empty.setAlignmentX(Component.LEFT_ALIGNMENT);
        categoryResultsPanel.add(empty);
    }

    private void refreshCategoryResultsPanel() {
        categoryResultsPanel.revalidate();
        categoryResultsPanel.repaint();
        if (categoryResultsScroll != null) {
            SwingUtilities.invokeLater(() -> categoryResultsScroll.getVerticalScrollBar().setValue(0));
        }
    }

    private void viewSelectedCategory() {
        String s = categoryList.getSelectedValue();
        if (s == null) return;

        showCategoryByName(selectedCategoryNameFromListValue(s));
    }

    private void showCategoryByName(String cat) {
        if (cat == null || cat.trim().isEmpty()) return;
        cat = cat.trim();

        refreshCategories();
        for (int i = 0; i < categoryModel.size(); i++) {
            if (cat.equals(selectedCategoryNameFromListValue(categoryModel.getElementAt(i)))) {
                categoryList.setSelectedIndex(i);
                categoryList.ensureIndexIsVisible(i);
                break;
            }
        }

        showCategoryDetails(cat);
        renderSelectedCategoryResults();
        showCard("categories");
    }

    private void showCategoryDetails(String cat) {
        JPanel panel = ensureDetailsPanel();
        panel.removeAll();
        addDetailTitle("Category: " + cat);
        addDetailText(currentProfile.categories.getOrDefault(cat, "") + "\nHighlight color: " + colorHex(colorForCategory(cat)));

        for (TextAnnotation a : currentProfile.annotations) {
            if (cat.equals(a.category)) {
                JButton b = blackButton(a.sourceTitle + " — “" + shorten(a.selectedText, 70) + "”");
                b.setAlignmentX(Component.LEFT_ALIGNMENT);
                b.addActionListener(e -> {
                    openSourceForAnnotation(a);
                    safeSelect(a.start, a.end);
                    showAnnotationDetails(a);
                });
                panel.add(b);
                panel.add(Box.createVerticalStrut(6));
            }
        }
        for (ChapterNote n : currentProfile.chapterNotes.values()) {
            if (n != null && n.linkedCategoryNames.contains(cat)) {
                JButton b = blackButton("Chapter Note: " + chapterNoteReference(n) + " — “" + shortenLines(n.noteText, 70) + "”");
                b.setAlignmentX(Component.LEFT_ALIGNMENT);
                b.addActionListener(e -> openChapterNoteViewer(n));
                panel.add(b);
                panel.add(Box.createVerticalStrut(6));
            }
        }

        panel.revalidate();
        panel.repaint();
    }

    private void openSourceForAnnotation(TextAnnotation a) {
        if (a.sourceKey.startsWith("BIBLE:")) {
            String ref = a.sourceKey.substring("BIBLE:".length()) + ":1";
            RefParts rp = parseRef(ref);
            if (rp != null) {
                selectedBook = rp.book;
                selectedChapter = rp.chapter;
                refreshBookCombo();
                showSelectedChapter(false);
            }
        } else if (a.sourceKey.startsWith("LIBRARY:")) {
            showLibraryDoc(a.sourceKey.substring("LIBRARY:".length()));
        }
    }

    private void addQuestionForSelection() {
        String[] choices = {"Discussion Question", "Personal Question"};
        int choice = JOptionPane.showOptionDialog(this, "What type of question do you want to add?", "Question Type",
                JOptionPane.DEFAULT_OPTION, JOptionPane.QUESTION_MESSAGE, null, choices, choices[0]);
        if (choice < 0) return;
        addAnnotationFromSelection("Question", choice == 1 ? "personal" : "discussion");
    }

    private void refreshStudyProjects() {
        if (currentProfile == null) return;
        if (currentProfile.studyProjects == null) currentProfile.studyProjects = new TreeMap<>();
        if (studyProjectModel == null) return;
        StudyProject selected = studyProjectList == null ? null : studyProjectList.getSelectedValue();
        String selectedId = selected == null ? "" : safe(selected.id);
        studyProjectModel.clear();
        String projectQuery = studyProjectSearchField == null || studyProjectSearchField.getClientProperty("projectFilter") == null ? "" : studyProjectSearchField.getClientProperty("projectFilter").toString().trim().toLowerCase(Locale.ROOT);
        for (StudyProject project : currentProfile.studyProjects.values()) {
            repairStudyProject(project);
            if (!projectQuery.isEmpty() && !(safe(project.title) + " " + safe(project.description)).toLowerCase(Locale.ROOT).contains(projectQuery)) continue;
            studyProjectModel.addElement(project);
        }
        if (studyProjectModel.size() > 0 && studyProjectList != null) {
            int idx = 0;
            for (int i = 0; i < studyProjectModel.size(); i++) {
                if (safe(studyProjectModel.get(i).id).equals(selectedId)) { idx = i; break; }
            }
            studyProjectList.setSelectedIndex(idx);
        }
        renderSelectedStudyProject();
    }

    private StudyProject selectedStudyProject() {
        return studyProjectList == null ? null : studyProjectList.getSelectedValue();
    }

    private void createStudyProject() {
        StudyProject p = promptForStudyProject(null);
        if (p == null) return;
        currentProfile.studyProjects.put(p.id, p);
        saveData();
        refreshStudyProjects();
        if (studyProjectList != null) studyProjectList.setSelectedValue(p, true);
        log("Created study project: " + p.title);
    }

    private void editSelectedStudyProject() {
        StudyProject selected = selectedStudyProject();
        if (selected == null) return;
        StudyProject edited = promptForStudyProject(selected);
        if (edited == null) return;
        selected.title = edited.title;
        selected.description = edited.description;
        selected.updatedAt = System.currentTimeMillis();
        saveData();
        refreshStudyProjects();
    }

    private void deleteSelectedStudyProject() {
        StudyProject selected = selectedStudyProject();
        if (selected == null) return;
        if (JOptionPane.showConfirmDialog(this, "Delete study project '" + selected.title + "'?", "Delete Project", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
        currentProfile.studyProjects.remove(selected.id);
        saveData();
        refreshStudyProjects();
    }

    private StudyProject promptForStudyProject(StudyProject existing) {
        JTextField title = new JTextField(existing == null ? "" : safe(existing.title));
        JTextArea desc = new JTextArea(existing == null ? "" : safe(existing.description), 5, 38);
        desc.setLineWrap(true);
        desc.setWrapStyleWord(true);
        JPanel p = new JPanel(new GridLayout(0, 1, 5, 5));
        p.add(new JLabel("Project title:")); p.add(title);
        p.add(new JLabel("Description:")); p.add(new JScrollPane(desc));
        int r = JOptionPane.showConfirmDialog(this, p, existing == null ? "Create Study Project" : "Edit Study Project", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION || title.getText().trim().isEmpty()) return null;
        long now = System.currentTimeMillis();
        StudyProject project = existing == null ? new StudyProject() : new StudyProject();
        project.id = existing == null ? UUID.randomUUID().toString() : existing.id;
        project.title = title.getText().trim();
        project.description = desc.getText().trim();
        project.createdAt = existing == null || existing.createdAt <= 0L ? now : existing.createdAt;
        project.updatedAt = now;
        return project;
    }

    private StudyProject chooseStudyProject(boolean allowCreate) {
        if (currentProfile == null) return null;
        if (currentProfile.studyProjects == null) currentProfile.studyProjects = new TreeMap<>();
        List<String> names = new ArrayList<>();
        Map<String, StudyProject> byName = new LinkedHashMap<>();
        for (StudyProject p : currentProfile.studyProjects.values()) {
            String label = p.title == null || p.title.trim().isEmpty() ? p.id : p.title;
            names.add(label);
            byName.put(label, p);
        }
        if (allowCreate) names.add("+ Create New Study Project");
        if (names.isEmpty()) {
            StudyProject created = promptForStudyProject(null);
            if (created != null) {
                currentProfile.studyProjects.put(created.id, created);
                return created;
            }
            return null;
        }
        Object choice = JOptionPane.showInputDialog(this, "Choose study project:", "Study Project", JOptionPane.PLAIN_MESSAGE, null, names.toArray(), names.get(0));
        if (choice == null) return null;
        if (choice.toString().startsWith("+ Create")) {
            StudyProject created = promptForStudyProject(null);
            if (created != null) currentProfile.studyProjects.put(created.id, created);
            return created;
        }
        return byName.get(choice.toString());
    }

    private void renderSelectedStudyProject() {
        if (studyProjectDetailsPanel == null) return;
        studyProjectDetailsPanel.removeAll();
        StudyProject p = selectedStudyProject();
        if (p == null) {
            JLabel empty = new JLabel("No study project selected. Create a project to collect notes, bookmarks, and memory verses.");
            empty.setAlignmentX(Component.LEFT_ALIGNMENT);
            JButton createEmpty = blackButton("Create Project");
            createEmpty.setAlignmentX(Component.LEFT_ALIGNMENT);
            createEmpty.addActionListener(e -> createStudyProject());
            studyProjectDetailsPanel.add(empty);
            studyProjectDetailsPanel.add(Box.createVerticalStrut(8));
            studyProjectDetailsPanel.add(createEmpty);
            studyProjectDetailsPanel.revalidate(); studyProjectDetailsPanel.repaint();
            return;
        }
        addProjectTitle(p.title);
        addProjectText(safe(p.description).isEmpty() ? "No description yet." : p.description);
        JButton export = blackButton("Export Study Project");
        export.setAlignmentX(Component.LEFT_ALIGNMENT);
        export.addActionListener(e -> exportStudyProject(p));
        studyProjectDetailsPanel.add(export);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(10));

        addProjectSection("Project Notes");
        if (p.projectNotes.isEmpty()) addProjectText("No project notes yet.");
        for (ProjectNote n : p.projectNotes) addProjectNoteRow(p, n);

        addProjectSection("Attached Annotations / Highlights");
        boolean anyAnnotation = false;
        for (String id : p.annotationIds) {
            TextAnnotation a = findAnnotationById(id);
            if (a != null) { anyAnnotation = true; addProjectAnnotationRow(a); }
        }
        if (!anyAnnotation) addProjectText("No attached annotations yet.");

        addProjectSection("Chapter Notes");
        boolean anyChapterNote = false;
        for (String id : p.chapterNoteIds) {
            ChapterNote n = findChapterNoteById(id);
            if (n != null) { anyChapterNote = true; addProjectChapterNoteRow(n); }
        }
        if (!anyChapterNote) addProjectText("No attached chapter notes yet.");

        addProjectSection("Bookmarks");
        boolean anyBookmark = false;
        for (String id : p.bookmarkIds) {
            StudyBookmark b = findBookmarkById(id);
            if (b != null) { anyBookmark = true; addProjectBookmarkRow(b); }
        }
        if (!anyBookmark) addProjectText("No attached bookmarks yet.");

        studyProjectDetailsPanel.revalidate();
        studyProjectDetailsPanel.repaint();
    }

    private void addProjectChapterNoteRow(ChapterNote n) {
        JButton b = blackButton(chapterNoteReference(n) + " — “" + shortenLines(n.noteText, 90) + "”");
        b.setAlignmentX(Component.LEFT_ALIGNMENT);
        b.addActionListener(e -> openChapterNoteViewer(n));
        studyProjectDetailsPanel.add(b);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(6));
    }

    private void addProjectTitle(String text) {
        JLabel l = new JLabel("<html><b>" + esc(text) + "</b></html>");
        l.setFont(new Font("Segoe UI", Font.BOLD, 22));
        l.setForeground(darkRed);
        l.setAlignmentX(Component.LEFT_ALIGNMENT);
        studyProjectDetailsPanel.add(l);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(8));
    }

    private void addProjectSection(String text) {
        JLabel l = new JLabel("<html><b>" + esc(text) + "</b></html>");
        l.setFont(new Font("Segoe UI", Font.BOLD, 16));
        l.setForeground(darkRed);
        l.setAlignmentX(Component.LEFT_ALIGNMENT);
        studyProjectDetailsPanel.add(l);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(5));
    }

    private void addProjectText(String text) {
        JTextArea a = readonlyArea();
        a.setText(text == null ? "" : text);
        a.setAlignmentX(Component.LEFT_ALIGNMENT);
        studyProjectDetailsPanel.add(a);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(6));
    }

    private void addProjectNoteRow(StudyProject p, ProjectNote n) {
        JPanel row = projectRow("Project Note", safe(n.title), safe(n.sourceTitle), safe(n.selectedText) + (safe(n.body).isEmpty() ? "" : "\n" + n.body));
        JButton open = blackButton("Open");
        open.addActionListener(e -> openProjectNote(n));
        row.add(open, BorderLayout.EAST);
        studyProjectDetailsPanel.add(row);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(6));
    }

    private void addProjectAnnotationRow(TextAnnotation a) {
        JPanel row = projectRow(safe(a.type), safe(a.category), safe(a.sourceTitle), safe(a.selectedText) + (safe(a.note).isEmpty() ? "" : "\n" + a.note));
        JButton open = blackButton("Open");
        open.addActionListener(e -> { openSourceForAnnotation(a); safeSelect(a.start, a.end); showAnnotationDetails(a); showCard("study"); });
        row.add(open, BorderLayout.EAST);
        studyProjectDetailsPanel.add(row);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(6));
    }

    private void addProjectBookmarkRow(StudyBookmark b) {
        JPanel row = projectRow("Bookmark", safe(b.title), safe(b.sourceTitle), safe(b.previewText));
        JButton open = blackButton("Open");
        open.addActionListener(e -> openBookmark(b));
        row.add(open, BorderLayout.EAST);
        studyProjectDetailsPanel.add(row);
        studyProjectDetailsPanel.add(Box.createVerticalStrut(6));
    }

    private JPanel projectRow(String type, String title, String source, String preview) {
        JPanel row = new JPanel(new BorderLayout(8, 4));
        row.setBackground(cream);
        row.setBorder(new CompoundBorder(new LineBorder(new Color(210, 185, 160)), new EmptyBorder(6, 6, 6, 6)));
        JLabel label = new JLabel("<html><b>" + esc(type) + (safe(title).isEmpty() ? "" : ": " + esc(title)) + "</b><br>" + esc(source) + "<br><i>" + esc(shorten(preview, 220)) + "</i></html>");
        row.add(label, BorderLayout.CENTER);
        row.setAlignmentX(Component.LEFT_ALIGNMENT);
        return row;
    }

    private void addSelectedTextToStudyProject() {
        if (readerPane == null || currentProfile == null) return;
        int start = readerPane.getSelectionStart();
        int end = readerPane.getSelectionEnd();
        String selected = readerPane.getSelectedText();
        if (end <= start || selected == null || selected.trim().isEmpty()) {
            JOptionPane.showMessageDialog(this, "Select text first, then choose Add Selected Text To Study Project.");
            return;
        }
        StudyProject project = chooseStudyProject(true);
        if (project == null) return;
        JTextArea body = new JTextArea(5, 38);
        body.setLineWrap(true); body.setWrapStyleWord(true);
        int r = JOptionPane.showConfirmDialog(this, new JScrollPane(body), "Optional project note", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;
        ProjectNote note = new ProjectNote();
        long now = System.currentTimeMillis();
        note.id = UUID.randomUUID().toString();
        note.title = defaultBookmarkTitle(currentSourceTitle, selected);
        note.body = body.getText().trim();
        note.sourceKey = currentSourceKey;
        note.sourceTitle = currentSourceTitle;
        note.selectedText = selected;
        note.start = start;
        note.end = end;
        note.createdAt = now;
        note.updatedAt = now;
        project.projectNotes.add(note);
        project.updatedAt = now;
        saveData();
        refreshStudyProjects();
        showDetailsText("Added selected text to study project: " + project.title + "\n\n" + selected);
        log("Added selected text to study project: " + project.title);
    }

    private void addAnnotationToStudyProject(TextAnnotation a) {
        if (a == null) return;
        StudyProject p = chooseStudyProject(true);
        if (p == null) return;
        if (!p.annotationIds.contains(a.id)) p.annotationIds.add(a.id);
        p.updatedAt = System.currentTimeMillis();
        saveData();
        refreshStudyProjects();
        showDetailsText("Added note/highlight to study project: " + p.title + "\n\n" + a.selectedText);
        log("Added note to study project: " + p.title);
    }

    private void addBookmarkToStudyProject(StudyBookmark b) {
        if (b == null) return;
        StudyProject p = chooseStudyProject(true);
        if (p == null) return;
        if (!p.bookmarkIds.contains(b.id)) p.bookmarkIds.add(b.id);
        p.updatedAt = System.currentTimeMillis();
        saveData();
        refreshStudyProjects();
        log("Added bookmark to study project: " + p.title);
    }

    private void addPinnedItemToStudyProject(PinnedItem item) {
        if (item == null) return;
        if (!safe(item.annotationId).isEmpty()) {
            TextAnnotation a = findAnnotationById(item.annotationId);
            if (a != null) { addAnnotationToStudyProject(a); return; }
        }
        StudyProject p = chooseStudyProject(true);
        if (p == null) return;
        ProjectNote note = new ProjectNote();
        long now = System.currentTimeMillis();
        note.id = UUID.randomUUID().toString();
        note.title = "Pinned item";
        note.body = item.note;
        note.sourceKey = item.sourceKey;
        note.sourceTitle = item.sourceTitle;
        note.selectedText = item.selectedText;
        note.start = item.start;
        note.end = item.end;
        note.createdAt = now;
        note.updatedAt = now;
        p.projectNotes.add(note);
        p.updatedAt = now;
        saveData();
        refreshStudyProjects();
        log("Added pinned item to study project: " + p.title);
    }

    private void searchSelectedStudyProject() {
        if (studyProjectSearchModel == null) return;
        studyProjectSearchModel.clear();
        StudyProject p = selectedStudyProject();
        String q = studyProjectSearchField == null ? "" : studyProjectSearchField.getText().trim().toLowerCase(Locale.ROOT);
        if (p == null || q.isEmpty()) return;
        for (ProjectNote n : p.projectNotes) if (matchesProjectNote(n, q)) studyProjectSearchModel.addElement(resultForProjectNote(p, n));
        for (String id : p.annotationIds) {
            TextAnnotation a = findAnnotationById(id);
            if (a != null && matchesAnnotation(a, q)) studyProjectSearchModel.addElement(resultForAnnotation(p, a));
        }
    }

    private void searchAllStudyNotes() {
        if (allNotesSearchModel == null) return;
        allNotesSearchModel.clear();
        String q = allNotesSearchField == null ? "" : allNotesSearchField.getText().trim().toLowerCase(Locale.ROOT);
        if (q.isEmpty()) return;
        for (TextAnnotation a : currentProfile.annotations) if (matchesAnnotation(a, q)) allNotesSearchModel.addElement(resultForAnnotation(null, a));
        for (ChapterNote n : currentProfile.chapterNotes.values()) if (chapterNoteSearchText(n).contains(q)) allNotesSearchModel.addElement(resultForChapterNote(null, n));
        for (StudyProject p : currentProfile.studyProjects.values()) {
            for (ProjectNote n : p.projectNotes) if (matchesProjectNote(n, q)) allNotesSearchModel.addElement(resultForProjectNote(p, n));
        }
    }

    private boolean matchesProjectNote(ProjectNote n, String q) {
        return (safe(n.title) + " " + safe(n.body) + " " + safe(n.selectedText) + " " + safe(n.sourceTitle)).toLowerCase(Locale.ROOT).contains(q);
    }

    private boolean matchesAnnotation(TextAnnotation a, String q) {
        return (safe(a.selectedText) + " " + safe(a.note) + " " + safe(a.category) + " " + safe(a.target) + " " + safe(a.sourceTitle)).toLowerCase(Locale.ROOT).contains(q);
    }

    private StudySearchResult resultForProjectNote(StudyProject p, ProjectNote n) {
        StudySearchResult r = new StudySearchResult();
        r.type = "Project Note";
        r.projectId = p == null ? "" : p.title;
        r.itemId = n.id;
        r.sourceKey = n.sourceKey;
        r.sourceTitle = n.sourceTitle;
        r.start = n.start;
        r.end = n.end;
        r.title = n.title;
        r.preview = shorten((safe(n.title).isEmpty() ? "" : n.title + " — ") + safe(n.selectedText) + " " + safe(n.body), 180);
        return r;
    }

    private StudySearchResult resultForChapterNote(StudyProject p, ChapterNote n) {
        StudySearchResult r = new StudySearchResult();
        r.type = "Chapter Note";
        r.projectId = p == null ? "" : p.title;
        r.itemId = n.id;
        r.sourceKey = n.sourceKey;
        r.sourceTitle = n.sourceTitle;
        r.start = 0;
        r.end = 0;
        r.title = chapterNoteReference(n);
        r.preview = shortenLines(n.noteText, 180);
        return r;
    }

    private StudySearchResult resultForAnnotation(StudyProject p, TextAnnotation a) {
        StudySearchResult r = new StudySearchResult();
        r.type = "Annotation";
        r.projectId = p == null ? "" : p.title;
        r.itemId = a.id;
        r.sourceKey = a.sourceKey;
        r.sourceTitle = a.sourceTitle;
        r.start = a.start;
        r.end = a.end;
        r.title = a.type;
        r.preview = shorten(safe(a.selectedText) + " " + safe(a.note), 180);
        return r;
    }

    private void openStudySearchResult(StudySearchResult r) {
        if (r == null) return;
        if ("Annotation".equals(r.type)) {
            TextAnnotation a = findAnnotationById(r.itemId);
            if (a != null) { openSourceForAnnotation(a); safeSelect(a.start, a.end); showAnnotationDetails(a); showCard("study"); }
        } else if ("Chapter Note".equals(r.type)) {
            ChapterNote n = findChapterNoteById(r.itemId);
            if (n != null) openChapterNoteViewer(n);
        } else {
            ProjectNote n = findProjectNoteById(r.itemId);
            if (n != null) openProjectNote(n);
        }
    }

    private void openProjectNote(ProjectNote n) {
        if (n == null) return;
        if (!safe(n.sourceKey).isEmpty()) openSourceKey(n.sourceKey);
        safeSelect(n.start, n.end);
        JPanel panel = ensureDetailsPanel();
        panel.removeAll();
        addDetailTitle(safe(n.title).isEmpty() ? "Project Note" : n.title);
        addDetailText("Source: " + safe(n.sourceTitle) + "\nSelected text: “" + safe(n.selectedText) + "”\nCreated: " + displayDate(n.createdAt) + "\nUpdated: " + displayDate(n.updatedAt));
        if (!safe(n.body).isEmpty()) addDetailText(n.body);
        panel.revalidate(); panel.repaint();
        showCard("study");
    }

    private StudyBookmark findBookmarkById(String id) {
        if (id == null || currentProfile == null || currentProfile.bookmarks == null) return null;
        for (StudyBookmark b : currentProfile.bookmarks) if (b != null && id.equals(b.id)) return b;
        return null;
    }

    private ProjectNote findProjectNoteById(String id) {
        if (id == null || currentProfile == null || currentProfile.studyProjects == null) return null;
        for (StudyProject p : currentProfile.studyProjects.values()) {
            for (ProjectNote n : p.projectNotes) if (id.equals(n.id)) return n;
        }
        return null;
    }

    private void exportStudyProject(StudyProject p) {
        if (p == null) return;
        JFileChooser ch = new JFileChooser();
        ch.setSelectedFile(new File(p.title.replaceAll("[^a-zA-Z0-9._-]+", "_") + ".txt"));
        if (ch.showSaveDialog(this) != JFileChooser.APPROVE_OPTION) return;
        try (PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(ch.getSelectedFile()), StandardCharsets.UTF_8))) {
            out.println("Study Project: " + p.title);
            out.println("Description: " + safe(p.description));
            out.println("Created: " + displayDate(p.createdAt));
            out.println("Updated: " + displayDate(p.updatedAt));
            out.println();
            out.println("PROJECT NOTES"); out.println("=============");
            for (ProjectNote n : p.projectNotes) {
                out.println("- " + safe(n.title));
                out.println("  Source: " + safe(n.sourceTitle));
                out.println("  Selected: " + safe(n.selectedText));
                if (!safe(n.body).isEmpty()) out.println("  Note: " + safe(n.body));
                out.println();
            }
            out.println("ANNOTATIONS / HIGHLIGHTS"); out.println("========================");
            for (String id : p.annotationIds) {
                TextAnnotation a = findAnnotationById(id);
                if (a == null) continue;
                out.println("- " + safe(a.type) + " | " + safe(a.sourceTitle));
                out.println("  Selected: " + safe(a.selectedText));
                out.println("  Category: " + safe(a.category));
                out.println("  Target: " + safe(a.target));
                out.println("  Note / Greek notes: " + safe(a.note));
                out.println();
            }
            out.println("BOOKMARKS"); out.println("=========");
            for (String id : p.bookmarkIds) {
                StudyBookmark b = findBookmarkById(id);
                if (b == null) continue;
                out.println("- " + safe(b.title) + " | " + safe(b.sourceTitle) + " | " + safe(b.type));
                out.println("  Preview: " + safe(b.previewText));
                out.println();
            }
            out.println("MEMORY VERSES"); out.println("=============");
            for (String id : p.memoryVerseIds) {
                MemoryVerse mv = findMemoryVerseById(id);
                if (mv == null) continue;
                out.println("- " + safe(mv.reference) + " | " + safe(mv.category));
                out.println("  " + safe(mv.text));
                if (!safe(mv.note).isEmpty()) out.println("  Note: " + safe(mv.note));
                out.println();
            }
            log("Exported study project to " + ch.getSelectedFile().getAbsolutePath());
        } catch (Exception ex) { showError("Export study project failed", ex); }
    }

    private MemoryVerse findMemoryVerseById(String id) {
        if (id == null || currentProfile == null || currentProfile.memoryVerses == null) return null;
        for (MemoryVerse mv : currentProfile.memoryVerses) if (mv != null && id.equals(mv.id)) return mv;
        return null;
    }


    private void refreshQuestions() {
        if (questionModel == null) return;
        questionModel.clear();
        if (discussionQuestionsPanel != null) discussionQuestionsPanel.removeAll();
        if (personalQuestionsPanel != null) personalQuestionsPanel.removeAll();
        String questionQuery = questionSearchField == null ? "" : questionSearchField.getText().trim().toLowerCase(Locale.ROOT);
        int discussionCount = 0;
        int personalCount = 0;
        for (int i = 0; i < currentProfile.questions.size(); i++) {
            StudyQuestion q = currentProfile.questions.get(i);
            repairQuestion(q);
            String line = i + " | " + (q.answered ? "✓" : "❗") + " | " + questionTypeDisplay(q.questionType) + " | " + questionLocation(q) + " | " + shorten(q.question, 140);
            if (!questionQuery.isEmpty() && !questionSearchText(q).contains(questionQuery)) continue;
            questionModel.addElement(line);
            JPanel target = "personal".equals(normalizeQuestionType(q.questionType)) ? personalQuestionsPanel : discussionQuestionsPanel;
            if (target != null) target.add(buildQuestionCard(q, i));
            if ("personal".equals(normalizeQuestionType(q.questionType))) personalCount++; else discussionCount++;
        }
        if (discussionQuestionsPanel != null && discussionCount == 0) discussionQuestionsPanel.add(emptySectionLabel("No discussion questions match this filter."));
        if (personalQuestionsPanel != null && personalCount == 0) personalQuestionsPanel.add(emptySectionLabel("No personal questions match this filter."));
        if (discussionQuestionsPanel != null) { discussionQuestionsPanel.revalidate(); discussionQuestionsPanel.repaint(); }
        if (personalQuestionsPanel != null) { personalQuestionsPanel.revalidate(); personalQuestionsPanel.repaint(); }
        updateHeader();
    }

    private JLabel emptySectionLabel(String text) {
        JLabel label = new JLabel(text);
        label.setForeground(modernMutedText);
        label.setBorder(new EmptyBorder(12, 12, 12, 12));
        label.setAlignmentX(Component.LEFT_ALIGNMENT);
        return label;
    }

    private JPanel buildQuestionCard(StudyQuestion q, int index) {
        JPanel card = new JPanel(new BorderLayout(8, 8));
        card.setBackground(new Color(255, 252, 247));
        card.setBorder(new CompoundBorder(new MatteBorder(0, 5, 0, 0, questionColorForType(q.questionType)), new EmptyBorder(8, 10, 8, 10)));
        card.setAlignmentX(Component.LEFT_ALIGNMENT);
        JTextArea text = readonlyArea();
        text.setBackground(card.getBackground());
        text.setText(q.question + "\n\nSource: " + questionLocation(q) + "\nStatus: " + (q.answered ? "Answered" : "Unanswered") + " • " + q.answers.size() + " answer(s)" + answersSummary(q));
        card.add(text, BorderLayout.CENTER);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 0));
        actions.setOpaque(false);
        JButton jump = new JButton("Jump To");
        jump.addActionListener(e -> jumpToQuestion(q));
        JButton addAnswer = new JButton("Add Answer");
        addAnswer.addActionListener(e -> promptAddAnswer(q));
        JButton viewAnswers = new JButton("View Answers");
        viewAnswers.addActionListener(e -> showQuestionAnswers(q));
        JButton edit = new JButton("Edit Question");
        edit.addActionListener(e -> editQuestion(q));
        JButton delete = new JButton("Delete Question");
        delete.addActionListener(e -> deleteQuestion(q));
        JButton toggle = new JButton(q.answered ? "Mark Unanswered" : "Mark Answered");
        toggle.addActionListener(e -> { selectedQuestion = q; toggleSelectedQuestion(); });
        for (JButton b : new JButton[]{jump, addAnswer, viewAnswers, edit, delete, toggle}) actions.add(b);
        card.add(actions, BorderLayout.SOUTH);
        card.addMouseListener(new MouseAdapter() { public void mouseClicked(MouseEvent e) { selectedQuestion = q; if (questionList != null && index >= 0 && index < questionModel.size()) questionList.setSelectedIndex(index); } });
        return card;
    }

    private String answersSummary(StudyQuestion q) {
        if (q.answers.isEmpty()) return "";
        StringBuilder sb = new StringBuilder("\n\nAnswers:");
        for (QuestionAnswer a : q.answers) sb.append("\n- ").append(displayDate(a.createdAt)).append(": ").append(shorten(safe(a.text), 160));
        return sb.toString();
    }

    private String questionSearchText(StudyQuestion q) {
        return (safe(q.sourceTitle) + " " + safe(q.selectedText) + " " + safe(q.question) + " " + questionLocation(q) + " " + questionTypeDisplay(q.questionType)).toLowerCase(Locale.ROOT);
    }

    private String questionLocation(StudyQuestion q) {
        if (q == null) return "Unknown";
        if (q.wholeChapter && !safe(q.book).isEmpty() && q.chapter > 0) return q.book + " " + q.chapter;
        return safe(q.sourceTitle).isEmpty() ? safe(q.sourceKey) : safe(q.sourceTitle);
    }

    private StudyQuestion questionForAnnotation(String annotationId) {
        if (currentProfile == null || annotationId == null) return null;
        for (StudyQuestion q : currentProfile.questions) if (q != null && annotationId.equals(q.annotationId)) return q;
        return null;
    }

    private void addAnswerToQuestion(StudyQuestion q, String text) {
        if (q == null || text == null || text.trim().isEmpty()) return;
        repairQuestion(q);
        q.answers.add(new QuestionAnswer(text.trim()));
        q.answered = true;
        saveData();
    }

    private void promptAddAnswer(StudyQuestion q) {
        String answer = promptMultiline("Add Answer", "Answer for: " + shorten(q.question, 80), "");
        if (answer == null || answer.trim().isEmpty()) return;
        addAnswerToQuestion(q, answer);
        refreshQuestions();
    }

    private void showQuestionAnswers(StudyQuestion q) {
        JTextArea area = readonlyArea();
        area.setText(q.question + "\n\n" + (q.answers.isEmpty() ? "No answers yet." : answersSummary(q)));
        JOptionPane.showMessageDialog(this, new JScrollPane(area), "Answers", JOptionPane.INFORMATION_MESSAGE);
    }

    private void editQuestion(StudyQuestion q) {
        String edited = promptMultiline("Edit Question", "Question:", q.question);
        if (edited == null || edited.trim().isEmpty()) return;
        q.question = edited.trim();
        TextAnnotation a = annotationById(q.annotationId);
        if (a != null) { a.note = q.question; touchAnnotation(a); }
        saveData();
        refreshQuestions();
        reloadCurrentSource();
    }

    private void deleteQuestion(StudyQuestion q) {
        if (q == null) return;
        int r = JOptionPane.showConfirmDialog(this, "Delete this question and its answers?", "Delete Question", JOptionPane.OK_CANCEL_OPTION);
        if (r != JOptionPane.OK_OPTION) return;
        currentProfile.questions.remove(q);
        currentProfile.annotations.removeIf(a -> a != null && safe(q.annotationId).equals(safe(a.id)));
        saveData();
        refreshQuestions();
        refreshRecentNotes();
        reloadCurrentSource();
    }

    private void jumpToQuestion(StudyQuestion q) {
        TextAnnotation a = annotationById(q.annotationId);
        if (a != null) {
            openSourceForAnnotation(a);
            if (!a.wholeChapter) safeSelect(a.start, a.end);
            else moveReaderCaret(0);
            showAnnotationDetails(a);
            showCard("study");
        }
    }

    private TextAnnotation annotationById(String id) {
        if (currentProfile == null || id == null) return null;
        for (TextAnnotation a : currentProfile.annotations) if (a != null && id.equals(a.id)) return a;
        return null;
    }

    private void refreshRecentNotes() {
        refreshChapterNotesList();
        if (recentModel == null || currentProfile == null) return;
        recentModel.clear();

        String filter = recentFilterBox == null || recentFilterBox.getSelectedItem() == null
                ? "All" : recentFilterBox.getSelectedItem().toString();
        String query = recentSearchField == null ? "" : recentSearchField.getText().trim().toLowerCase(Locale.ROOT);

        List<TextAnnotation> annotations = new ArrayList<>(currentProfile.annotations);
        annotations.sort((a, b) -> Long.compare(annotationSortTime(b), annotationSortTime(a)));

        for (TextAnnotation a : annotations) {
            repairAnnotation(a, System.currentTimeMillis());
            if (isChapterLevelNote(a)) continue;
            if (!matchesRecentFilter(a, filter)) continue;
            if (!query.isEmpty() && !recentSearchText(a).contains(query)) continue;
            recentModel.addElement(new RecentAnnotationListItem(a));
        }
        if (recentModel.isEmpty() && statusLabel != null && currentProfile.annotations.isEmpty()) {
            statusLabel.setText(" Recent Notes is empty — highlighted notes, categories, Greek notes, attachments, and questions will appear here.");
        }
    }

    private void refreshChapterNotesList() {
        if (chapterNotesModel == null || currentProfile == null) return;
        repairProfile(currentProfile);
        chapterNotesModel.clear();
        String q = chapterNotesSearchField == null ? "" : chapterNotesSearchField.getText().trim().toLowerCase(Locale.ROOT);
        java.util.List<ChapterNote> notes = new ArrayList<>(currentProfile.chapterNotes.values());
        notes.removeIf(n -> n == null || safe(n.noteText).trim().isEmpty());
        for (TextAnnotation a : currentProfile.annotations) {
            repairAnnotation(a, System.currentTimeMillis());
            if (isChapterLevelNote(a) && !safe(a.note).trim().isEmpty()) notes.add(chapterNoteFromAnnotation(a));
        }
        notes.sort((a, b) -> Long.compare(b.updatedAt, a.updatedAt));
        for (ChapterNote n : notes) {
            repairChapterNote(n);
            if (!q.isEmpty() && !chapterNoteSearchText(n).contains(q)) continue;
            chapterNotesModel.addElement(n);
        }
    }

    private boolean isChapterLevelNote(TextAnnotation a) {
        if (a == null) return false;
        if (a.wholeChapter) return true;
        if ("Chapter Note".equalsIgnoreCase(safe(a.type))) return true;
        boolean hasNoRange = a.start <= 0 && a.end <= 0;
        boolean hasNoSelection = safe(a.selectedText).trim().isEmpty() && safe(a.target).trim().isEmpty();
        return hasNoRange && hasNoSelection;
    }

    private ChapterNote chapterNoteFromAnnotation(TextAnnotation a) {
        ChapterNote n = new ChapterNote();
        n.id = "annotation:" + safe(a.id);
        n.annotationId = safe(a.id);
        n.sourceKey = safe(a.sourceKey);
        n.sourceTitle = safe(a.sourceTitle);
        n.book = safe(a.book);
        n.chapter = a.chapter;
        n.noteText = safe(a.note);
        n.createdAt = a.createdAt > 0L ? a.createdAt : (a.created == null ? System.currentTimeMillis() : a.created.getTime());
        n.updatedAt = annotationSortTime(a);
        if (!safe(a.category).isEmpty()) n.linkedCategoryNames.add(a.category);
        return n;
    }

    private TextAnnotation annotationForChapterNote(ChapterNote note) {
        return note == null || safe(note.annotationId).isEmpty() ? null : annotationById(note.annotationId);
    }

    private String chapterNoteSearchText(ChapterNote n) {
        return (safe(n.book) + " " + n.chapter + " " + safe(n.sourceTitle) + " " + safe(n.sourceKey) + " " + safe(n.noteText) + " "
                + String.join(" ", n.linkedCategoryNames) + " " + String.join(" ", n.linkedStudyProjectNames) + " " + String.join(" ", n.linkedTopicPageNames)).toLowerCase(Locale.ROOT);
    }

    private java.util.List<ChapterNote> currentChapterNotesForReader() {
        java.util.List<ChapterNote> notes = new ArrayList<>();
        if (currentProfile == null || safe(currentSourceKey).isEmpty()) return notes;
        repairProfile(currentProfile);
        String noteBook = currentSourceKey.startsWith("BIBLE:") ? selectedBook : "";
        int noteChapter = currentSourceKey.startsWith("BIBLE:") ? selectedChapter : 0;
        ChapterNote n = currentProfile.chapterNotes.get(chapterNoteKey(currentSourceKey, noteBook, noteChapter));
        if (n != null && !safe(n.noteText).trim().isEmpty()) notes.add(n);
        for (TextAnnotation a : currentProfile.annotations) {
            repairAnnotation(a, System.currentTimeMillis());
            if (currentSourceKey.equals(a.sourceKey) && isChapterLevelNote(a) && !safe(a.note).trim().isEmpty()) notes.add(chapterNoteFromAnnotation(a));
        }
        notes.sort((a, b) -> Long.compare(b.updatedAt, a.updatedAt));
        return notes;
    }

    private String chapterNoteKey(String sourceKey, String book, int chapter) {
        return safe(sourceKey) + "|" + safe(book) + "|" + chapter;
    }

    private ChapterNote getOrCreateChapterNote(String sourceKey, String sourceTitle, String book, int chapter) {
        repairProfile(currentProfile);
        String key = chapterNoteKey(sourceKey, book, chapter);
        ChapterNote note = currentProfile.chapterNotes.get(key);
        if (note == null) {
            note = new ChapterNote();
            note.id = UUID.randomUUID().toString();
            note.sourceKey = safe(sourceKey);
            note.sourceTitle = safe(sourceTitle);
            note.book = safe(book);
            note.chapter = chapter;
            note.createdAt = System.currentTimeMillis();
            note.updatedAt = note.createdAt;
            currentProfile.chapterNotes.put(key, note);
        }
        repairChapterNote(note);
        return note;
    }

    private ChapterNote findChapterNoteById(String id) {
        if (currentProfile == null || safe(id).isEmpty()) return null;
        repairProfile(currentProfile);
        for (ChapterNote n : currentProfile.chapterNotes.values()) if (n != null && safe(id).equals(n.id)) return n;
        return null;
    }

    private void openCurrentChapterNoteEditor() {
        if (currentProfile == null || safe(currentSourceKey).isEmpty()) return;
        String noteBook = currentSourceKey.startsWith("BIBLE:") ? selectedBook : "";
        int noteChapter = currentSourceKey.startsWith("BIBLE:") ? selectedChapter : 0;
        ChapterNote note = getOrCreateChapterNote(currentSourceKey, currentSourceTitle, noteBook, noteChapter);
        showChapterNoteDialog(note, true);
    }

    private void openChapterNoteViewer(ChapterNote note) {
        if (note == null) return;
        TextAnnotation annotation = annotationForChapterNote(note);
        if (annotation != null) {
            showChapterAnnotationDialog(annotation, false);
            return;
        }
        showChapterNoteDialog(note, false);
    }

    private void jumpToChapterNote(ChapterNote note) {
        if (note == null) return;
        TextAnnotation annotation = annotationForChapterNote(note);
        if (annotation != null) {
            openSourceForAnnotation(annotation);
            showCard("study");
            return;
        }
        if (!safe(note.book).isEmpty() && note.chapter > 0) {
            selectedBook = note.book;
            selectedChapter = note.chapter;
            refreshBookCombo();
            showSelectedChapter(false);
            showCard("study");
        } else if (!safe(note.sourceKey).isEmpty()) {
            openSourceKey(note.sourceKey);
            showCard("study");
        }
    }

    private boolean hasPersistedChapterNote(ChapterNote note) {
        if (note == null || currentProfile == null) return false;
        repairProfile(currentProfile);
        for (ChapterNote n : currentProfile.chapterNotes.values()) {
            if (n != null && safe(note.id).equals(n.id) && !safe(n.noteText).trim().isEmpty()) return true;
        }
        TextAnnotation annotation = annotationForChapterNote(note);
        return annotation != null && !safe(annotation.note).trim().isEmpty();
    }

    private void refreshAfterChapterNoteDelete() {
        refreshRecentNotes();
        refreshChapterNotesList();
        refreshMarginNotesPanel();
        refreshStudyProjects();
        refreshTopicPages();
        refreshCategories();
    }

    private void removeChapterNoteLinks(String noteId) {
        if (currentProfile == null || safe(noteId).isEmpty()) return;
        for (StudyProject project : currentProfile.studyProjects.values()) {
            if (project == null) continue;
            repairStudyProject(project);
            project.chapterNoteIds.removeIf(id -> safe(noteId).equals(safe(id)));
        }
        for (TopicPage topic : currentProfile.topicPages) {
            if (topic == null) continue;
            repairTopicPage(topic);
            topic.links.removeIf(link -> link != null
                    && "CHAPTER_NOTE".equalsIgnoreCase(safe(link.type))
                    && safe(noteId).equals(safe(link.ref)));
        }
    }

    private void performChapterNoteDelete(ChapterNote note) {
        if (note == null || currentProfile == null) return;
        repairProfile(currentProfile);
        String noteId = safe(note.id);
        TextAnnotation annotation = annotationForChapterNote(note);
        if (annotation != null) {
            String annotationId = safe(annotation.id);
            currentProfile.annotations.removeIf(a -> a != null && annotationId.equals(safe(a.id)));
            currentProfile.questions.removeIf(q -> q != null && annotationId.equals(safe(q.annotationId)));
        }
        currentProfile.chapterNotes.values().removeIf(n -> n != null && noteId.equals(safe(n.id)));
        removeChapterNoteLinks(noteId);
        saveData();
        refreshAfterChapterNoteDelete();
        if (statusLabel != null) statusLabel.setText(" Deleted chapter note for " + chapterNoteReference(note) + ".");
    }

    private void deleteChapterNote(ChapterNote note) {
        if (note == null || currentProfile == null) return;
        String ref = chapterNoteReference(note);
        if (!hasPersistedChapterNote(note)) {
            JOptionPane.showMessageDialog(this, "Nothing to delete.", "Delete Chapter Note", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        int ok = JOptionPane.showConfirmDialog(this,
                "Delete the chapter note for " + ref + "? This cannot be undone.",
                "Delete Chapter Note",
                JOptionPane.YES_NO_OPTION,
                JOptionPane.WARNING_MESSAGE);
        if (ok != JOptionPane.YES_OPTION) return;
        performChapterNoteDelete(note);
    }

    private void deleteChapterNoteFromEditor(ChapterNote note, JDialog dialog) {
        if (note == null || currentProfile == null) return;
        String ref = chapterNoteReference(note);
        if (!hasPersistedChapterNote(note)) {
            JOptionPane.showMessageDialog(dialog, "Nothing to delete.", "Delete Chapter Note", JOptionPane.INFORMATION_MESSAGE);
            return;
        }
        int ok = JOptionPane.showConfirmDialog(dialog,
                "Delete the chapter note for " + ref + "? This cannot be undone.",
                "Delete Chapter Note",
                JOptionPane.YES_NO_OPTION,
                JOptionPane.WARNING_MESSAGE);
        if (ok != JOptionPane.YES_OPTION) return;
        performChapterNoteDelete(note);
        if (dialog != null) dialog.dispose();
    }

    private void showChapterNoteDialog(ChapterNote note, boolean editMode) {
        if (note == null) return;
        repairChapterNote(note);
        final JDialog dialog = new JDialog(this, "Chapter Notes — " + chapterNoteReference(note), false);
        dialog.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
        dialog.setSize(850, 650);
        dialog.setLocationRelativeTo(this);

        JPanel root = new JPanel(new BorderLayout(10, 10));
        root.setBorder(new EmptyBorder(14, 14, 14, 14));
        root.setBackground(modernBackground);

        JLabel title = new JLabel("Chapter Notes — " + chapterNoteReference(note));
        title.setFont(new Font("Segoe UI", Font.BOLD, 22));
        title.setForeground(darkRed);
        root.add(title, BorderLayout.NORTH);

        JTextPane notePane = smartVerseTextPane(note, true);
        notePane.setText(safe(note.noteText));
        notePane.setEditable(editMode);
        notePane.setFont(new Font("Segoe UI", Font.PLAIN, 16));
        notePane.setBackground(modernSurface);
        notePane.setBorder(new EmptyBorder(12, 12, 12, 12));
        installChapterNotePopupMenu(notePane, note);
        notePane.getDocument().addDocumentListener(new SimpleDocumentListener(() -> { if (!Boolean.TRUE.equals(notePane.getClientProperty("applyingSmartVerseHighlights"))) applySmartVerseHighlights(notePane, note); }));
        SwingUtilities.invokeLater(() -> applySmartVerseHighlights(notePane, note));

        JScrollPane scroll = new JScrollPane(notePane);
        scroll.getVerticalScrollBar().setUnitIncrement(18);
        scroll.setBorder(new RoundedBorder(modernBorder, 12, new Insets(1, 1, 1, 1)));
        root.add(scroll, BorderLayout.CENTER);

        JLabel status = new JLabel(" ");
        status.setForeground(modernMutedText);

        final boolean[] dirty = {false};
        final JButton[] deleteButtonRef = {null};
        notePane.getDocument().addDocumentListener(new SimpleDocumentListener(() -> dirty[0] = true));

        Runnable save = () -> {
            String text = notePane.getText();
            note.noteText = text == null ? "" : text;
            note.updatedAt = System.currentTimeMillis();
            if (note.createdAt <= 0L) note.createdAt = note.updatedAt;
            currentProfile.chapterNotes.put(chapterNoteKey(note.sourceKey, note.book, note.chapter), note);
            saveData();
            dirty[0] = false;
            refreshRecentNotes();
            refreshMarginNotesPanel();
            refreshStudyProjects();
            refreshTopicPages();
            String msg = "Chapter notes saved for " + chapterNoteReference(note) + ".";
            status.setText(msg);
            if (statusLabel != null) statusLabel.setText(" " + msg);
            if (deleteButtonRef[0] != null) deleteButtonRef[0].setEnabled(hasPersistedChapterNote(note));
            applySmartVerseHighlights(notePane, note);
        };

        Runnable close = () -> {
            if (dirty[0]) {
                int choice = JOptionPane.showConfirmDialog(dialog, "Save changes before closing?", "Unsaved Chapter Notes", JOptionPane.YES_NO_CANCEL_OPTION);
                if (choice == JOptionPane.CANCEL_OPTION || choice == JOptionPane.CLOSED_OPTION) return;
                if (choice == JOptionPane.YES_OPTION) save.run();
            }
            dialog.dispose();
        };

        JButton mode = blackButton(editMode ? "View Mode" : "Edit Mode");
        mode.addActionListener(e -> {
            boolean editable = !notePane.isEditable();
            notePane.setEditable(editable);
            mode.setText(editable ? "View Mode" : "Edit Mode");
            status.setText(editable ? "Edit mode enabled." : "View mode enabled.");
        });
        JButton saveBtn = blackButton("Save");
        saveBtn.addActionListener(e -> save.run());
        JButton deleteBtn = blackButton("Delete");
        deleteBtn.setToolTipText("Delete this saved chapter note.");
        deleteBtn.setEnabled(hasPersistedChapterNote(note));
        deleteBtn.addActionListener(e -> deleteChapterNoteFromEditor(note, dialog));
        deleteButtonRef[0] = deleteBtn;
        JButton clear = blackButton("Clear Text");
        clear.addActionListener(e -> {
            int ok = JOptionPane.showConfirmDialog(dialog, "Clear this chapter note text?", "Clear Chapter Note Text", JOptionPane.YES_NO_OPTION);
            if (ok == JOptionPane.YES_OPTION) { notePane.setText(""); save.run(); }
        });
        JButton linkCat = blackButton("Link Whole Note to Category");
        linkCat.addActionListener(e -> linkWholeChapterNoteToCategory(note));
        JButton linkStudy = blackButton("Link Whole Note to Study Project");
        linkStudy.addActionListener(e -> linkWholeChapterNoteToStudy(note));
        JButton linkTopic = blackButton("Link Whole Note to Topic Page");
        linkTopic.addActionListener(e -> linkWholeChapterNoteToTopic(note));
        JButton closeBtn = blackButton("Close");
        closeBtn.addActionListener(e -> close.run());

        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 4));
        buttons.setOpaque(false);
        for (JButton b : new JButton[]{mode, saveBtn, deleteBtn, clear, linkCat, linkStudy, linkTopic, closeBtn}) buttons.add(b);
        JPanel south = new JPanel(new BorderLayout(8, 6));
        south.setOpaque(false);
        south.add(buttons, BorderLayout.CENTER);
        south.add(status, BorderLayout.SOUTH);
        root.add(south, BorderLayout.SOUTH);

        notePane.getInputMap(JComponent.WHEN_FOCUSED).put(KeyStroke.getKeyStroke(KeyEvent.VK_S, InputEvent.CTRL_DOWN_MASK), "saveChapterNote");
        notePane.getActionMap().put("saveChapterNote", new AbstractAction() { public void actionPerformed(ActionEvent e) { save.run(); }});
        root.getInputMap(JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT).put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "closeChapterNote");
        root.getActionMap().put("closeChapterNote", new AbstractAction() { public void actionPerformed(ActionEvent e) { close.run(); }});
        dialog.addWindowListener(new WindowAdapter() { public void windowClosing(WindowEvent e) { close.run(); }});
        dialog.setContentPane(root);
        dialog.setVisible(true);
    }

    private JTextPane smartVerseTextPane(ChapterNote note, boolean editable) {
        JTextPane pane = new JTextPane() {
            public String getToolTipText(MouseEvent event) {
                Integer start = smartVerseStartAt(this, event.getPoint());
                if (start == null) return super.getToolTipText(event);
                Object tip = getStyledDocument().getCharacterElement(start).getAttributes().getAttribute("verseTooltip");
                return tip == null ? super.getToolTipText(event) : tip.toString();
            }
        };
        ToolTipManager.sharedInstance().registerComponent(pane);
        pane.setEditable(editable);
        return pane;
    }

    private Integer smartVerseStartAt(JTextPane pane, Point point) {
        try {
            int pos = pane.viewToModel2D(point);
            if (pos < 0) return null;
            AttributeSet attrs = pane.getStyledDocument().getCharacterElement(pos).getAttributes();
            Object start = attrs.getAttribute("verseRefStart");
            return start instanceof Integer ? (Integer) start : null;
        } catch (Exception ignored) { return null; }
    }

    private void applySmartVerseHighlights(JTextPane pane, ChapterNote note) {
        if (pane == null || note == null) return;
        StyledDocument doc = pane.getStyledDocument();
        SwingUtilities.invokeLater(() -> {
            if (Boolean.TRUE.equals(pane.getClientProperty("applyingSmartVerseHighlights"))) return;
            pane.putClientProperty("applyingSmartVerseHighlights", Boolean.TRUE);
            try {
                String text = doc.getText(0, doc.getLength());
                SimpleAttributeSet normal = new SimpleAttributeSet();
                StyleConstants.setForeground(normal, modernText);
                StyleConstants.setBackground(normal, pane.getBackground());
                StyleConstants.setUnderline(normal, false);
                normal.addAttribute("verseTooltip", null);
                doc.setCharacterAttributes(0, doc.getLength(), normal, false);
                Matcher m = Pattern.compile("(?i)\\b(?:v\\.?|verses?\\s+)(\\d{1,3})(?:\\s*-\\s*(\\d{1,3}))?").matcher(text);
                while (m.find()) {
                    int startVerse = Integer.parseInt(m.group(1));
                    int endVerse = m.group(2) == null ? startVerse : Integer.parseInt(m.group(2));
                    SimpleAttributeSet ref = new SimpleAttributeSet();
                    StyleConstants.setForeground(ref, modernDarkRed);
                    StyleConstants.setBackground(ref, new Color(255, 244, 190));
                    StyleConstants.setUnderline(ref, true);
                    ref.addAttribute("verseTooltip", versePreviewTooltip(note, startVerse, endVerse));
                    ref.addAttribute("verseRefStart", m.start());
                    doc.setCharacterAttributes(m.start(), m.end() - m.start(), ref, false);
                }
            } catch (Exception ignored) {
            } finally {
                pane.putClientProperty("applyingSmartVerseHighlights", Boolean.FALSE);
            }
        });
    }

    private String versePreviewTooltip(ChapterNote note, int startVerse, int endVerse) {
        if (note == null || !safe(note.sourceKey).startsWith("BIBLE:")) return "Verse preview unavailable for this source.";
        if (endVerse < startVerse) { int t = startVerse; startVerse = endVerse; endVerse = t; }
        Map<Integer, Verse> verses = data.getVerses(note.book, note.chapter);
        if (verses == null || verses.isEmpty()) return "Verse preview unavailable for this source.";
        StringBuilder sb = new StringBuilder("<html><body style='width:360px'>");
        boolean found = false;
        for (int v = startVerse; v <= endVerse; v++) {
            Verse verse = verses.get(v);
            if (verse != null) {
                found = true;
                sb.append("<b>").append(esc(note.book)).append(' ').append(note.chapter).append(':').append(v).append("</b> ").append(esc(verse.text)).append("<br>");
            }
        }
        if (!found) return "Verse not found in this chapter.";
        sb.append("</body></html>");
        return sb.toString();
    }

    private void installChapterNotePopupMenu(JTextPane pane, ChapterNote note) {
        JPopupMenu menu = new JPopupMenu();
        JMenuItem cat = new JMenuItem("Attach Selected Note Text to Category");
        cat.addActionListener(e -> attachSelectedNoteText(note, pane, "CATEGORY"));
        JMenuItem study = new JMenuItem("Attach Selected Note Text to Study Project");
        study.addActionListener(e -> attachSelectedNoteText(note, pane, "STUDY"));
        JMenuItem topic = new JMenuItem("Attach Selected Note Text to Topic Page");
        topic.addActionListener(e -> attachSelectedNoteText(note, pane, "TOPIC"));
        JMenuItem copy = new JMenuItem("Copy");
        copy.addActionListener(e -> pane.copy());
        menu.add(cat); menu.add(study); menu.add(topic); menu.addSeparator(); menu.add(copy);
        pane.setComponentPopupMenu(menu);
    }

    private void attachSelectedNoteText(ChapterNote note, JTextPane pane, String type) {
        if (note == null || pane == null) return;
        int start = Math.min(pane.getSelectionStart(), pane.getSelectionEnd());
        int end = Math.max(pane.getSelectionStart(), pane.getSelectionEnd());
        if (start == end) { JOptionPane.showMessageDialog(this, "Select text inside the chapter note first."); return; }
        String selected = pane.getSelectedText();
        ChapterNoteHighlight h = new ChapterNoteHighlight();
        h.chapterNoteId = note.id; h.startOffset = start; h.endOffset = end; h.selectedText = safe(selected); h.type = type; h.createdAt = System.currentTimeMillis();
        if ("CATEGORY".equals(type)) {
            String cat = chooseCategoryName(true);
            if (cat == null) return;
            h.linkId = cat; h.linkName = cat;
            if (!note.linkedCategoryNames.contains(cat)) note.linkedCategoryNames.add(cat);
        } else if ("STUDY".equals(type)) {
            StudyProject p = chooseStudyProject(true);
            if (p == null) return;
            h.linkId = p.id; h.linkName = p.title;
            if (!note.linkedStudyProjectIds.contains(p.id)) note.linkedStudyProjectIds.add(p.id);
            if (!note.linkedStudyProjectNames.contains(p.title)) note.linkedStudyProjectNames.add(p.title);
            if (!p.chapterNoteIds.contains(note.id)) p.chapterNoteIds.add(note.id);
            p.updatedAt = System.currentTimeMillis();
        } else {
            TopicPage topic = chooseTopicPageWithCreateOption();
            if (topic == null) return;
            h.linkId = topic.id; h.linkName = topic.title;
            if (!note.linkedTopicPageIds.contains(topic.id)) note.linkedTopicPageIds.add(topic.id);
            if (!note.linkedTopicPageNames.contains(topic.title)) note.linkedTopicPageNames.add(topic.title);
            addLinkToTopic(topic, new LinkedItem("CHAPTER_NOTE", note.id, "selected note text"));
        }
        note.highlights.add(h);
        note.updatedAt = System.currentTimeMillis();
        saveData(); refreshRecentNotes(); refreshStudyProjects(); refreshTopicPages(); refreshMarginNotesPanel();
        if (statusLabel != null) statusLabel.setText(" Attached selected chapter-note text to " + h.linkName + ".");
    }

    private String chooseCategoryName(boolean allowCreate) {
        repairProfile(currentProfile);
        java.util.List<String> names = new ArrayList<>(currentProfile.categories.keySet());
        if (allowCreate) names.add(0, "Create New Category…");
        if (names.isEmpty()) names.add("Create New Category…");
        JComboBox<String> combo = new JComboBox<>(names.toArray(new String[0]));
        int result = JOptionPane.showConfirmDialog(this, combo, "Choose Category", JOptionPane.OK_CANCEL_OPTION);
        if (result != JOptionPane.OK_OPTION) return null;
        String selected = (String) combo.getSelectedItem();
        if ("Create New Category…".equals(selected)) {
            String name = JOptionPane.showInputDialog(this, "Category name:", "New Category", JOptionPane.PLAIN_MESSAGE);
            if (name == null || name.trim().isEmpty()) return null;
            currentProfile.categories.putIfAbsent(name.trim(), "");
            currentProfile.categoryColors.putIfAbsent(name.trim(), categoryBlue.getRGB());
            refreshCategories();
            return name.trim();
        }
        return selected;
    }

    private void linkWholeChapterNoteToCategory(ChapterNote note) {
        String cat = chooseCategoryName(true);
        if (cat == null) return;
        if (!note.linkedCategoryNames.contains(cat)) note.linkedCategoryNames.add(cat);
        note.updatedAt = System.currentTimeMillis(); saveData(); refreshRecentNotes(); refreshCategories(); refreshMarginNotesPanel();
    }

    private void linkWholeChapterNoteToStudy(ChapterNote note) {
        StudyProject p = chooseStudyProject(true);
        if (p == null) return;
        repairStudyProject(p);
        if (!p.chapterNoteIds.contains(note.id)) p.chapterNoteIds.add(note.id);
        if (!note.linkedStudyProjectIds.contains(p.id)) note.linkedStudyProjectIds.add(p.id);
        if (!note.linkedStudyProjectNames.contains(p.title)) note.linkedStudyProjectNames.add(p.title);
        p.updatedAt = note.updatedAt = System.currentTimeMillis(); saveData(); refreshRecentNotes(); refreshStudyProjects(); refreshMarginNotesPanel();
    }

    private void linkWholeChapterNoteToTopic(ChapterNote note) {
        TopicPage topic = chooseTopicPageWithCreateOption();
        if (topic == null) return;
        if (!note.linkedTopicPageIds.contains(topic.id)) note.linkedTopicPageIds.add(topic.id);
        if (!note.linkedTopicPageNames.contains(topic.title)) note.linkedTopicPageNames.add(topic.title);
        addLinkToTopic(topic, new LinkedItem("CHAPTER_NOTE", note.id, "whole chapter note"));
        note.updatedAt = System.currentTimeMillis(); saveData(); refreshRecentNotes(); refreshTopicPages(); refreshMarginNotesPanel();
    }

    private String chapterNoteReference(ChapterNote n) {
        if (n == null) return "Chapter Note";
        return (safe(n.book).isEmpty() ? shortBibleSourceTitle(n.sourceTitle) : getShortBookDisplayName(n.book) + " " + n.chapter);
    }

    private long annotationSortTime(TextAnnotation a) {
        if (a == null) return 0L;
        if (a.updatedAt > 0L) return a.updatedAt;
        if (a.createdAt > 0L) return a.createdAt;
        if (a.created != null) return a.created.getTime();
        return 0L;
    }

    private boolean matchesRecentFilter(TextAnnotation a, String filter) {
        if (a == null || filter == null || "All".equals(filter)) return true;
        if ("Notes".equals(filter)) return "Note".equals(a.type);
        if ("Categories".equals(filter)) return "Category".equals(a.type);
        if ("Questions".equals(filter)) return "Question".equals(a.type);
        if ("Greek".equals(filter)) return "Greek".equals(a.type);
        if ("Attachments".equals(filter)) return "Link".equals(a.type);
        return true;
    }

    private String recentSearchText(TextAnnotation a) {
        return (safe(a.selectedText) + " " + safe(a.note) + " " + safe(a.category) + " "
                + safe(a.sourceTitle) + " " + safe(a.sourceKey) + " " + safe(a.target))
                .toLowerCase(Locale.ROOT);
    }

    private void openSelectedRecentAnnotation() {
        RecentAnnotationListItem item = recentList == null ? null : recentList.getSelectedValue();
        if (item == null || item.annotation == null) return;
        openRecentAnnotation(item.annotation);
    }

    private void maybeShowRecentNoteContextMenu(MouseEvent e) {
        if (recentList == null || e == null || !e.isPopupTrigger()) return;
        int index = recentList.locationToIndex(e.getPoint());
        if (index < 0 || !recentList.getCellBounds(index, index).contains(e.getPoint())) return;
        recentList.setSelectedIndex(index);
        RecentAnnotationListItem item = recentList.getModel().getElementAt(index);
        TextAnnotation annotation = item == null ? null : item.annotation;
        if (annotation == null) return;
        JPopupMenu menu = new JPopupMenu();
        JMenuItem open = new JMenuItem("Open / Jump To");
        open.addActionListener(a -> openRecentAnnotation(annotation));
        JMenuItem edit = new JMenuItem("Edit Note");
        edit.addActionListener(a -> editAnnotation(annotation));
        JMenuItem delete = new JMenuItem("Delete Note");
        delete.addActionListener(a -> deleteAnnotation(annotation));
        menu.add(open);
        menu.add(edit);
        menu.add(delete);
        menu.show(recentList, e.getX(), e.getY());
        e.consume();
    }

    private void maybeShowChapterNoteContextMenu(MouseEvent e) {
        if (chapterNotesList == null || e == null || !e.isPopupTrigger()) return;
        int index = chapterNotesList.locationToIndex(e.getPoint());
        if (index < 0 || !chapterNotesList.getCellBounds(index, index).contains(e.getPoint())) return;
        chapterNotesList.setSelectedIndex(index);
        ChapterNote note = chapterNotesList.getModel().getElementAt(index);
        if (note == null) return;
        JPopupMenu menu = new JPopupMenu();
        JMenuItem open = new JMenuItem("Open Chapter Note");
        open.addActionListener(a -> openChapterNoteViewer(note));
        JMenuItem edit = new JMenuItem("Edit Chapter Note");
        edit.addActionListener(a -> editChapterNote(note));
        JMenuItem jump = new JMenuItem("Jump To Chapter");
        jump.addActionListener(a -> jumpToChapterNote(note));
        JMenuItem delete = new JMenuItem("Delete Chapter Note");
        delete.addActionListener(a -> deleteChapterNote(note));
        menu.add(open);
        menu.add(edit);
        menu.add(jump);
        menu.add(delete);
        menu.show(chapterNotesList, e.getX(), e.getY());
        e.consume();
    }

    private void openRecentAnnotation(TextAnnotation a) {
        if (a == null) return;
        openSourceForAnnotation(a);
        if (a.wholeChapter) moveReaderCaret(0);
        else safeSelect(a.start, a.end);
        showAnnotationDetails(a);
        showCard("study");
    }

    private void editChapterNote(ChapterNote note) {
        if (note == null) return;
        TextAnnotation annotation = annotationForChapterNote(note);
        if (annotation != null) showChapterAnnotationDialog(annotation, true);
        else showChapterNoteDialog(note, true);
    }

    private String displayDate(long millis) {
        if (millis <= 0L) return "Unknown";
        return new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(millis));
    }

    private String sourceTitleFor(TextAnnotation a) {
        if (a == null) return "";
        String title = a.sourceTitle == null || a.sourceTitle.trim().isEmpty() ? safe(a.sourceKey) : a.sourceTitle;
        if (safe(a.sourceKey).startsWith("BIBLE:")) return shortBibleReferenceTitle(a, title);
        return title;
    }

    private void touchAnnotation(TextAnnotation a) {
        if (a != null) a.updatedAt = System.currentTimeMillis();
    }

    private int countUnanswered() {
        int c = 0;
        for (StudyQuestion q : currentProfile.questions) if (!q.answered) c++;
        return c;
    }

    private void toggleSelectedQuestion() {
        String s = questionList.getSelectedValue();
        if (s == null) return;
        try {
            int idx = Integer.parseInt(s.split("\\|")[0].trim());
            StudyQuestion q = currentProfile.questions.get(idx);
            q.answered = !q.answered;
            saveData();
            refreshQuestions();
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this, "Could not toggle question.");
        }
    }

    private void toggleSideSearch() {
        sideSearchExpanded = !sideSearchExpanded;
        if (sideSearchBody != null) sideSearchBody.setVisible(sideSearchExpanded);
        if (sideSearchToggleBtn != null) sideSearchToggleBtn.setText(sideSearchExpanded ? "Minimize" : "Restore");
        if (sideSearchPanel != null) {
            sideSearchPanel.setPreferredSize(sideSearchExpanded
                    ? new Dimension(RIGHT_SIDEBAR_CONTENT_WIDTH, RIGHT_SIDEBAR_SEARCH_HEIGHT)
                    : null);
            sideSearchPanel.setMinimumSize(sideSearchExpanded
                    ? new Dimension(0, RIGHT_SIDEBAR_SEARCH_MIN_HEIGHT)
                    : new Dimension(0, 0));
            sideSearchPanel.revalidate();
            sideSearchPanel.repaint();
        }
    }

    private void doGreekSearch() {
        if (greekSearchModel == null) return;
        greekSearchModel.clear();
        lastGreekSearchQuery = greekSearchField == null ? "" : greekSearchField.getText().trim();
        if (greekSearchPreview != null) greekSearchPreview.setText("Type a Greek word, reference, morphology tag, or details text, then click Search.");
        if (greekSearchStatus != null) greekSearchStatus.setText(" ");
        if (data.greek == null || data.greek.isEmpty()) {
            if (greekSearchPreview != null) greekSearchPreview.setText("No Greek text has been imported yet. Open the Import tab and use Download + Import MorphGNT Greek, import a MorphGNT ZIP/TXT folder, or import a Greek CSV.");
            if (greekSearchStatus != null) greekSearchStatus.setText("No Greek imported");
            return;
        }
        if (lastGreekSearchQuery.isEmpty()) return;

        int total = fillGreekSearchModel(greekSearchModel, lastGreekSearchQuery, 300, 120);
        if (greekSearchStatus != null) {
            if (total > 300) {
                greekSearchStatus.setText("Showing first 300 results. Refine your search for more specific matches. (" + total + " total matches)");
            } else {
                greekSearchStatus.setText(total + (total == 1 ? " result" : " results"));
            }
        }
        if (greekSearchModel.isEmpty() && greekSearchPreview != null) greekSearchPreview.setText("No Greek entries found for: " + lastGreekSearchQuery);
        if (!greekSearchModel.isEmpty()) greekSearchList.setSelectedIndex(0);
    }

    private int fillGreekSearchModel(DefaultListModel<String> model, String query, int limit, int maxSnippet) {
        String q = safe(query).trim().toLowerCase(Locale.ROOT);
        if (q.isEmpty()) return 0;
        int matches = 0;
        for (GreekEntry ge : data.greek.values()) {
            String haystack = (ge.key() + " " + ge.greekText + " " + ge.details).toLowerCase(Locale.ROOT);
            if (!haystack.contains(q)) continue;
            matches++;
            if (model.size() < limit) model.addElement(ge.key() + " | " + shorten(ge.greekText, maxSnippet));
        }
        return matches;
    }

    private void previewGreekSearchSelection() {
        if (greekSearchList == null || greekSearchPreview == null) return;
        String s = greekSearchList.getSelectedValue();
        if (s == null) return;
        String key = greekSearchKeyFromLine(s);
        GreekEntry ge = data.greek.get(key);
        if (ge == null) return;
        greekSearchPreview.setText(greekDialogText(key, englishVerseTextFromData(key), ge.greekText, ge.details));
        greekSearchPreview.setCaretPosition(0);
        highlightTextPaneMatches(greekSearchPreview, lastGreekSearchQuery, null);
    }

    private void openGreekSearchSelection() {
        if (greekSearchList == null) return;
        String s = greekSearchList.getSelectedValue();
        if (s == null) return;
        openGreekResultVerse(greekSearchKeyFromLine(s), true);
    }

    private String greekSearchKeyFromLine(String line) {
        if (line == null) return "";
        int split = line.indexOf('|');
        return (split < 0 ? line : line.substring(0, split)).trim();
    }

    private void doSideSearch() {
        if (sideSearchModel == null) return;
        sideSearchModel.clear();
        String raw = sideSearchField == null ? "" : sideSearchField.getText().trim();
        String q = raw.toLowerCase(Locale.ROOT);
        if (q.isEmpty()) {
            sideSearchPreview.setText("Type a search above.");
            return;
        }

        PassageRef passage = parseBibleReferenceOrRange(raw);
        if (passage != null) {
            String text = getPassageText(passage.book, passage.chapter, passage.startVerse, passage.endVerse);
            if (!text.isEmpty()) sideSearchModel.addElement("PASSAGE|" + passage.display() + "|" + passage.key() + "|" + shorten(text, 120));
        }

        addCategorySideSearchResults(q, 120);
        fillSearchModel(sideSearchModel, q, 120);
        sideSearchPreview.setText(sideSearchModel.isEmpty() ? "No results found." : sideSearchModel.size() + " result(s). Click to preview. Double-click or right-click > Show Full View to open.");
    }

    private void addCategorySideSearchResults(String q, int maxSnippet) {
        if (currentProfile == null) return;
        ensureCategoryColors();
        for (String cat : currentProfile.categories.keySet()) {
            if (!cat.toLowerCase(Locale.ROOT).contains(q)) continue;
            sideSearchModel.addElement("CATEGORY|" + cat + "|" + cat + "|" + shorten(currentProfile.categories.getOrDefault(cat, ""), maxSnippet));
            for (TextAnnotation a : currentProfile.annotations) {
                if (cat.equals(a.category)) {
                    sideSearchModel.addElement("CATEGORY_ITEM|" + a.id + "|" + a.sourceTitle + "|" + shorten(a.selectedText + " — " + a.note, maxSnippet));
                }
            }
        }
    }

    private void previewSideSearchResult() {
        String s = sideSearchList.getSelectedValue();
        if (s == null) return;
        SearchResultParts p = parseSearchLine(s);
        sideSearchPreview.setText(previewForSearchResult(p));
        sideSearchPreview.setCaretPosition(0);
    }

    private void showSideSearchMenu(MouseEvent e) {
        if (sideSearchList == null) return;
        int idx = sideSearchList.locationToIndex(e.getPoint());
        if (idx >= 0) sideSearchList.setSelectedIndex(idx);
        if (sideSearchList.getSelectedValue() == null) return;

        SearchResultParts p = parseSearchLine(sideSearchList.getSelectedValue());
        JPopupMenu m = new JPopupMenu();
        if ("GREEK".equals(p.type)) {
            addMenu(m, "View Greek Details", () -> showGreekDetailsInSidebar(p.ref));
            addMenu(m, "Show Full Verse", () -> openGreekResultVerse(p.ref, true));
            addMenu(m, "Add Greek Note", () -> addGreekNoteFromSearchResult(p.ref));
            addMenu(m, "Add Greek to Topic Page", () -> addLinkedItemToTopicPage(new LinkedItem("GREEK", p.ref, "related")));
        } else {
            addMenu(m, "View In Sidebar", this::previewSideSearchResult);
            addMenu(m, "Show Full View", this::showFullViewForSideSearchResult);
        }
        m.show(sideSearchList, e.getX(), e.getY());
    }

    private void showFullViewForSideSearchResult() {
        String s = sideSearchList.getSelectedValue();
        if (s == null) return;
        openSearchLineFullView(s);
    }

    private void doSearch() {
        if (searchModel == null) return;
        searchModel.clear();
        String q = searchField.getText().trim().toLowerCase(Locale.ROOT);
        if (q.isEmpty()) return;
        fillSearchModel(searchModel, q, 150);
    }

    private void fillSearchModel(DefaultListModel<String> model, String q, int maxSnippet) {
        for (String book : data.bible.keySet()) {
            for (Integer ch : data.getChapters(book)) {
                for (Verse v : data.getVerses(book, ch).values()) {
                    if ((v.key() + " " + v.text).toLowerCase(Locale.ROOT).contains(q)) {
                        model.addElement("BIBLE|" + v.key() + "|" + v.key() + "|" + shorten(v.text, maxSnippet));
                    }
                }
            }
        }

        for (LibraryDoc d : data.libraryDocs) {
            if ((d.title + " " + d.body).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("LIBRARY|" + d.title + "|" + d.title + "|" + shorten(snippet(d.body, q), maxSnippet));
            }
        }

        for (GreekEntry ge : data.greek.values()) {
            String english = englishVerseTextFromData(ge.key());
            if ((ge.key() + " " + ge.greekText + " " + ge.details + " " + english).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("GREEK|" + ge.key() + "|" + ge.key() + "|" + shorten(ge.greekText + " — " + ge.details, maxSnippet));
            }
        }

        for (TextAnnotation a : currentProfile.annotations) {
            if ((a.sourceTitle + " " + a.selectedText + " " + a.type + " " + a.category + " " + a.note + " " + a.target).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("NOTE|" + a.id + "|" + a.sourceTitle + "|" + shorten(a.selectedText + " — " + a.note, maxSnippet));
            }
        }

        for (StudyQuestion qu : currentProfile.questions) {
            if ((qu.sourceTitle + " " + qu.selectedText + " " + qu.question).toLowerCase(Locale.ROOT).contains(q)) {
                model.addElement("NOTE|" + qu.annotationId + "|" + qu.sourceTitle + "|" + shorten(qu.question, maxSnippet));
            }
        }
    }

    private void openSearchResult() {
        String s = searchList.getSelectedValue();
        if (s == null) return;
        openSearchLineFullView(s);
    }

    private SearchResultParts parseSearchLine(String s) {
        String[] p = s.split("\\|", 4);
        return new SearchResultParts(
                p.length > 0 ? p[0].trim() : "",
                p.length > 1 ? p[1].trim() : "",
                p.length > 2 ? p[2].trim() : "",
                p.length > 3 ? p[3].trim() : ""
        );
    }

    private String previewForSearchResult(SearchResultParts p) {
        StringBuilder sb = new StringBuilder();
        sb.append(p.type).append("\n----------------------\n");

        if (p.type.equals("PASSAGE")) {
            PassageRef passage = parseBibleReferenceOrRange(p.ref);
            sb.append(p.ref).append("\n\n");
            sb.append(passage == null ? p.extra : getPassageText(passage.book, passage.chapter, passage.startVerse, passage.endVerse));
        } else if (p.type.equals("CATEGORY")) {
            sb.append("CATEGORY: ").append(p.ref).append("\n\n").append(currentProfile.categories.getOrDefault(p.ref, ""));
            int count = 0;
            for (TextAnnotation a : currentProfile.annotations) if (p.ref.equals(a.category)) count++;
            sb.append("\n\nAttached items: ").append(count);
        } else if (p.type.equals("BIBLE")) {
            Verse v = data.findVerse(p.ref);
            sb.append(p.ref).append("\n\n");
            sb.append(v == null ? p.extra : v.text);
        } else if (p.type.equals("GREEK")) {
            GreekEntry ge = data.greek.get(p.ref);
            sb.append(p.ref).append("\n\n");
            if (ge != null) {
                sb.append("Greek Text:\n").append(ge.greekText).append("\n\nDetails:\n").append(ge.details);
            } else {
                sb.append(p.extra);
            }
        } else if (p.type.equals("LIBRARY")) {
            sb.append(p.ref).append("\n\n").append(p.extra);
        } else if (p.type.equals("NOTE") || p.type.equals("CATEGORY_ITEM")) {
            TextAnnotation a = findAnnotationById(p.ref);
            if (a != null) {
                sb.append(a.sourceTitle).append("\n\nSelected Text:\n").append(a.selectedText).append("\n\n");
                if (!a.category.isEmpty()) sb.append("Category: ").append(a.category).append("\n\n");
                if (!a.target.isEmpty()) sb.append("Attached To: ").append(a.target).append("\n\n");
                sb.append(a.note);
            } else {
                sb.append(p.title).append("\n\n").append(p.extra);
            }
        } else {
            sb.append(p.extra);
        }

        return sb.toString();
    }

    private void openSearchLineFullView(String s) {
        SearchResultParts p = parseSearchLine(s);
        if (p.type.equals("PASSAGE") || p.type.equals("BIBLE")) {
            openTarget(p.ref);
            return;
        }
        if (p.type.equals("CATEGORY")) {
            showCategoryByName(p.ref);
            return;
        }
        if (p.type.equals("GREEK")) {
            openGreekResultVerse(p.ref, true);
            return;
        }
        if (p.type.equals("LIBRARY")) {
            showLibraryDoc(p.ref);
            showCard("study");
            return;
        }
        if (p.type.equals("NOTE") || p.type.equals("CATEGORY_ITEM")) {
            TextAnnotation a = findAnnotationById(p.ref);
            if (a != null) {
                openSourceForAnnotation(a);
                safeSelect(a.start, a.end);
                showAnnotationDetails(a);
                showCard("study");
            }
        }
    }

    private TextAnnotation findAnnotationById(String id) {
        if (id == null || currentProfile == null) return null;
        for (TextAnnotation a : currentProfile.annotations) if (id.equals(a.id)) return a;
        return null;
    }

    private void safeSelect(int start, int end) {
        int len = readerPane.getDocument().getLength();
        int originalLen = readerOriginalLength();
        int renderedStart = start;
        int renderedEnd = end;
        if (!visibleAnnotationBubbles.isEmpty() && start >= 0 && end >= start && end <= originalLen) {
            renderedStart = sourceOffsetToRenderedOffset(start, true);
            renderedEnd = sourceOffsetToRenderedOffset(end, false);
        }
        int s = Math.max(0, Math.min(renderedStart, len));
        int e = Math.max(s, Math.min(renderedEnd, len));
        readerPane.requestFocusInWindow();
        readerPane.select(s, e);
        readerPane.setCaretPosition(s);
    }

    private ParsedReference parseBibleReference(String input) {
        if (data == null || data.bible == null || data.bible.isEmpty()) return null;
        String normalized = normalizeReferenceInput(input);
        if (normalized.isEmpty()) return null;

        Map<String, String> aliases = buildBookAliasMap();
        List<String> sortedAliases = new ArrayList<>(aliases.keySet());
        sortedAliases.sort((a, b) -> Integer.compare(b.length(), a.length()));

        for (String alias : sortedAliases) {
            if (!normalized.equals(alias) && !normalized.startsWith(alias + " ")) continue;
            String remaining = normalized.length() == alias.length() ? "" : normalized.substring(alias.length()).trim();
            Matcher matcher = Pattern.compile("^(\\d+)(?:\\s*:\\s*(\\d+))?$").matcher(remaining);
            if (!matcher.matches()) continue;
            try {
                String bookKey = aliases.get(alias);
                int chapter = Integer.parseInt(matcher.group(1));
                Integer verse = matcher.group(2) == null ? null : Integer.parseInt(matcher.group(2));
                if (chapter <= 0 || (verse != null && verse <= 0)) return null;
                if (!data.bible.containsKey(bookKey) || !data.getChapters(bookKey).contains(chapter)) return null;
                if (verse != null && !data.getVerses(bookKey, chapter).containsKey(verse)) return null;
                return new ParsedReference(bookKey, chapter, verse);
            } catch (Exception ignored) {
                return null;
            }
        }
        return null;
    }

    private Map<String, String> buildBookAliasMap() {
        Map<String, String> aliases = new HashMap<>();
        if (data == null || data.bible == null) return aliases;

        for (String bookKey : data.bible.keySet()) {
            addBookAlias(aliases, bookKey, bookKey);
            addBookAlias(aliases, displayBibleBookName(bookKey), bookKey);
        }

        addCanonicalBookAliases(aliases, "Genesis", "Gen", "Ge", "Gn");
        addCanonicalBookAliases(aliases, "Exodus", "Ex", "Exo", "Exod");
        addCanonicalBookAliases(aliases, "Leviticus", "Lev", "Le", "Lv");
        addCanonicalBookAliases(aliases, "Numbers", "Num", "Nu", "Nm", "Nb");
        addCanonicalBookAliases(aliases, "Deuteronomy", "Deut", "Dt");
        addCanonicalBookAliases(aliases, "Joshua", "Josh", "Jos", "Jsh");
        addCanonicalBookAliases(aliases, "Judges", "Judg", "Jdg", "Jg");
        addCanonicalBookAliases(aliases, "Ruth", "Ru");
        addCanonicalBookAliases(aliases, "1 Samuel", "1 Sam", "1Sa", "1 Sm", "I Samuel", "First Samuel");
        addCanonicalBookAliases(aliases, "2 Samuel", "2 Sam", "2Sa", "2 Sm", "II Samuel", "Second Samuel");
        addCanonicalBookAliases(aliases, "1 Kings", "1 Kgs", "1Ki", "1 Kin", "I Kings", "First Kings");
        addCanonicalBookAliases(aliases, "2 Kings", "2 Kgs", "2Ki", "2 Kin", "II Kings", "Second Kings");
        addCanonicalBookAliases(aliases, "1 Chronicles", "1 Chr", "1Ch", "1 Chron", "I Chronicles", "First Chronicles");
        addCanonicalBookAliases(aliases, "2 Chronicles", "2 Chr", "2Ch", "2 Chron", "II Chronicles", "Second Chronicles");
        addCanonicalBookAliases(aliases, "Ezra", "Ezr");
        addCanonicalBookAliases(aliases, "Nehemiah", "Neh", "Ne");
        addCanonicalBookAliases(aliases, "Esther", "Est", "Esth");
        addCanonicalBookAliases(aliases, "Job", "Jb");
        addCanonicalBookAliases(aliases, "Psalms", "Psalm", "Ps", "Psa", "Pss");
        addCanonicalBookAliases(aliases, "Proverbs", "Prov", "Pr", "Prv");
        addCanonicalBookAliases(aliases, "Ecclesiastes", "Eccl", "Ecc", "Qoheleth");
        addCanonicalBookAliases(aliases, "Song of Solomon", "Song", "Song of Songs", "SOS", "Canticles");
        addCanonicalBookAliases(aliases, "Isaiah", "Isa", "Is");
        addCanonicalBookAliases(aliases, "Jeremiah", "Jer", "Je", "Jr");
        addCanonicalBookAliases(aliases, "Lamentations", "Lam", "La");
        addCanonicalBookAliases(aliases, "Ezekiel", "Ezek", "Eze", "Ez");
        addCanonicalBookAliases(aliases, "Daniel", "Dan", "Da", "Dn");
        addCanonicalBookAliases(aliases, "Hosea", "Hos", "Ho");
        addCanonicalBookAliases(aliases, "Joel", "Jl");
        addCanonicalBookAliases(aliases, "Amos", "Am");
        addCanonicalBookAliases(aliases, "Obadiah", "Obad", "Ob");
        addCanonicalBookAliases(aliases, "Jonah", "Jon");
        addCanonicalBookAliases(aliases, "Micah", "Mic", "Mc");
        addCanonicalBookAliases(aliases, "Nahum", "Nah", "Na");
        addCanonicalBookAliases(aliases, "Habakkuk", "Hab");
        addCanonicalBookAliases(aliases, "Zephaniah", "Zeph", "Zep");
        addCanonicalBookAliases(aliases, "Haggai", "Hag", "Hg");
        addCanonicalBookAliases(aliases, "Zechariah", "Zech", "Zec", "Zc");
        addCanonicalBookAliases(aliases, "Malachi", "Mal", "Ml");
        addCanonicalBookAliases(aliases, "Matthew", "Matt", "Mt");
        addCanonicalBookAliases(aliases, "Mark", "Mrk", "Mk", "Mr");
        addCanonicalBookAliases(aliases, "Luke", "Luk", "Lk");
        addCanonicalBookAliases(aliases, "John", "Jn", "Jhn", "Joh");
        addCanonicalBookAliases(aliases, "Acts", "Act", "Ac");
        addCanonicalBookAliases(aliases, "Romans", "Rom", "Ro", "Rm");
        addCanonicalBookAliases(aliases, "1 Corinthians", "1 Cor", "1Co", "I Corinthians", "First Corinthians");
        addCanonicalBookAliases(aliases, "2 Corinthians", "2 Cor", "2Co", "II Corinthians", "Second Corinthians");
        addCanonicalBookAliases(aliases, "Galatians", "Gal", "Ga");
        addCanonicalBookAliases(aliases, "Ephesians", "Eph", "Ep");
        addCanonicalBookAliases(aliases, "Philippians", "Phil", "Php", "Pp");
        addCanonicalBookAliases(aliases, "Colossians", "Col", "Co");
        addCanonicalBookAliases(aliases, "1 Thessalonians", "1 Thess", "1Th", "I Thessalonians", "First Thessalonians");
        addCanonicalBookAliases(aliases, "2 Thessalonians", "2 Thess", "2Th", "II Thessalonians", "Second Thessalonians");
        addCanonicalBookAliases(aliases, "1 Timothy", "1 Tim", "1Ti", "I Timothy", "First Timothy");
        addCanonicalBookAliases(aliases, "2 Timothy", "2 Tim", "2Ti", "II Timothy", "Second Timothy");
        addCanonicalBookAliases(aliases, "Titus", "Tit", "Ti");
        addCanonicalBookAliases(aliases, "Philemon", "Philem", "Phm", "Pm");
        addCanonicalBookAliases(aliases, "Hebrews", "Heb");
        addCanonicalBookAliases(aliases, "James", "Jas", "Jm");
        addCanonicalBookAliases(aliases, "1 Peter", "1 Pet", "1Pe", "1 Pt", "I Peter", "First Peter");
        addCanonicalBookAliases(aliases, "2 Peter", "2 Pet", "2Pe", "2 Pt", "II Peter", "Second Peter");
        addCanonicalBookAliases(aliases, "1 John", "1 Jn", "1Jn", "1Jo", "I John", "First John");
        addCanonicalBookAliases(aliases, "2 John", "2 Jn", "2Jn", "2Jo", "II John", "Second John");
        addCanonicalBookAliases(aliases, "3 John", "3 Jn", "3Jn", "3Jo", "III John", "Third John");
        addCanonicalBookAliases(aliases, "Jude", "Jud");
        addCanonicalBookAliases(aliases, "Revelation", "Rev", "Re", "The Revelation", "Apocalypse");
        return aliases;
    }

    private void addCanonicalBookAliases(Map<String, String> aliases, String canonical, String... variants) {
        String bookKey = findBibleBookKey(canonical);
        if (bookKey == null || bookKey.isEmpty()) return;
        addBookAlias(aliases, canonical, bookKey);
        for (String variant : variants) addBookAlias(aliases, variant, bookKey);
    }

    private void addBookAlias(Map<String, String> aliases, String alias, String bookKey) {
        String normalized = normalizeReferenceBookAlias(alias);
        if (normalized.isEmpty() || bookKey == null || bookKey.isEmpty()) return;
        aliases.putIfAbsent(normalized, bookKey);
        String compact = normalized.replace(" ", "");
        if (!compact.equals(normalized)) aliases.putIfAbsent(compact, bookKey);
    }

    private String findBibleBookKey(String canonical) {
        if (data == null || data.bible == null) return "";
        if (data.bible.containsKey(canonical)) return canonical;
        String wanted = normalizeReferenceBookAlias(canonical);
        for (String bookKey : data.bible.keySet()) {
            if (normalizeReferenceBookAlias(bookKey).equals(wanted)
                    || normalizeReferenceBookAlias(displayBibleBookName(bookKey)).equals(wanted)) return bookKey;
        }
        return "";
    }

    private String normalizeReferenceInput(String input) {
        if (input == null) return "";
        String normalized = input.toLowerCase(Locale.ROOT)
                .replace('’', '\'')
                .replaceAll("[.;,!?]+$", "")
                .replaceAll("[.]", "")
                .replaceAll("[^a-z0-9:']+", " ")
                .replaceAll("\\s*:\\s*", ":")
                .replaceAll("\\s+", " ")
                .trim();
        normalized = normalized.replace("'", " ").replaceAll("\\s+", " ").trim();
        normalized = normalized.replaceFirst("^first ", "1 ")
                .replaceFirst("^second ", "2 ")
                .replaceFirst("^third ", "3 ");
        return normalized.replaceAll("\\s+", " ").trim();
    }

    private String normalizeReferenceBookAlias(String alias) {
        String normalized = normalizeReferenceInput(alias);
        return normalized.replace(":", " ").replaceAll("\\s+", " ").trim();
    }

    private void goToBibleReference(String bookKey, int chapter, Integer verse) {
        selectedBook = bookKey;
        selectedChapter = chapter;
        refreshBookCombo();
        showSelectedChapter(true);
        showCard("study");
        if (verse != null) {
            SwingUtilities.invokeLater(() -> scrollToVerse(verse));
        }
        setStatusMessage("Opened " + displayBibleBookName(bookKey) + " " + chapter + (verse == null ? "" : ":" + verse) + ".");
    }

    private void scrollToVerse(int verse) {
        try {
            String txt = readerPane.getDocument().getText(0, readerPane.getDocument().getLength());
            Matcher marker = Pattern.compile("(?m)^" + Pattern.quote(String.valueOf(verse)) + "\\s").matcher(txt);
            if (!marker.find()) {
                setStatusMessage("Opened " + currentSourceTitle + ", but verse " + verse + " was not found in this chapter.");
                return;
            }
            int start = marker.start();
            int end = txt.indexOf("\n\n", start);
            if (end < 0) end = txt.length();
            readerPane.requestFocusInWindow();
            readerPane.setCaretPosition(start);
            readerPane.moveCaretPosition(end);
            Rectangle r = readerPane.modelToView2D(start).getBounds();
            readerPane.scrollRectToVisible(r);
        } catch (Exception ignored) {}
    }

    private void setStatusMessage(String message) {
        if (statusLabel == null || message == null) return;
        statusLabel.setText(" " + message);
    }

    private ChapterRef parseChapterRef(String key) {
        try {
            if (key == null || key.contains(":")) return null;
            key = key.trim().replaceAll("\\s+", " ");
            int sp = key.lastIndexOf(' ');
            if (sp < 0) return null;
            String book = normalizeBookName(key.substring(0, sp).trim());
            int chapter = Integer.parseInt(key.substring(sp + 1).trim());
            if (!data.getChapters(book).contains(chapter)) return null;
            return new ChapterRef(book, chapter);
        } catch (Exception e) {
            return null;
        }
    }

    private RefParts parseRef(String key) {
        try {
            key = key.trim().replaceAll("\\s+", " ");
            int colon = key.lastIndexOf(':');
            if (colon < 0) return null;
            String left = key.substring(0, colon).trim();
            int verse = Integer.parseInt(key.substring(colon + 1).trim());
            int sp = left.lastIndexOf(' ');
            if (sp < 0) return null;
            String book = normalizeBookName(left.substring(0, sp).trim());
            int chapter = Integer.parseInt(left.substring(sp + 1).trim());
            return new RefParts(book, chapter, verse);
        } catch (Exception e) {
            return null;
        }
    }

    private String normalizeBookName(String s) {
        if (s == null) return "";
        s = s.trim().replaceAll("\\s+", " ");
        String cleaned = s.replace(".", "");
        Map<String, String> aliases = new HashMap<>();
        aliases.put("Gen", "Genesis"); aliases.put("Ex", "Exodus"); aliases.put("Exo", "Exodus"); aliases.put("Lev", "Leviticus"); aliases.put("Num", "Numbers"); aliases.put("Deut", "Deuteronomy");
        aliases.put("Josh", "Joshua"); aliases.put("Judg", "Judges"); aliases.put("Ps", "Psalms"); aliases.put("Psa", "Psalms"); aliases.put("Prov", "Proverbs"); aliases.put("Eccl", "Ecclesiastes");
        aliases.put("Song", "Song of Solomon"); aliases.put("Isa", "Isaiah"); aliases.put("Jer", "Jeremiah"); aliases.put("Lam", "Lamentations"); aliases.put("Ezek", "Ezekiel"); aliases.put("Dan", "Daniel");
        aliases.put("Matt", "Matthew"); aliases.put("Mt", "Matthew"); aliases.put("Mk", "Mark"); aliases.put("Lk", "Luke"); aliases.put("Jn", "John"); aliases.put("Rom", "Romans"); aliases.put("Rev", "Revelation");
        for (String b : canonicalBooks()) if (b.equalsIgnoreCase(cleaned)) return b;
        for (Map.Entry<String, String> e : aliases.entrySet()) if (e.getKey().equalsIgnoreCase(cleaned)) return e.getValue();
        return s;
    }

    private List<String> canonicalBooks() {
        List<String> out = new ArrayList<>(bibleBookOrder().keySet());
        List<String> result = new ArrayList<>();
        for (String lower : out) {
            StringBuilder sb = new StringBuilder();
            for (String part : lower.split(" ")) {
                if (sb.length() > 0) sb.append(' ');
                if (part.length() == 1 && Character.isDigit(part.charAt(0))) sb.append(part);
                else sb.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
            result.add(sb.toString());
        }
        return result;
    }

    private String snippet(String body, String q) {
        String l = body.toLowerCase(Locale.ROOT);
        int i = l.indexOf(q);
        if (i < 0) return shorten(body, 180);
        int st = Math.max(0, i - 70);
        int en = Math.min(body.length(), i + 140);
        return body.substring(st, en).replace("\n", " ");
    }

    private String safe(String s) {
        return s == null ? "" : s;
    }

    private String shorten(String s, int max) {
        if (s == null) return "";
        s = s.replace("\r", " ").replace("\n", " ").trim();
        return s.length() <= max ? s : s.substring(0, Math.max(0, max)) + "...";
    }

    private String esc(String s) {
        if (s == null) return "";
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private String normalizeForFind(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT).replaceAll("\\s+", " ").trim();
    }

    private void downloadAndImportBsbUsfm() {
        new Thread(() -> {
            try {
                log("Downloading official BSB USFM ZIP...");
                byte[] bytes = download(OFFICIAL_BSB_USFM_ZIP);
                log("Downloaded " + bytes.length + " bytes. Importing...");
                int count = importUsfmZipBytes(bytes);
                log("Imported " + count + " BSB verses. Saved permanently.");
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("Download/import BSB failed", ex);
            }
        }).start();
    }

    private void downloadAndImportMorphGnt() {
        new Thread(() -> {
            try {
                log("Downloading MorphGNT/SBLGNT ZIP...");
                byte[] bytes = download(MORPHGNT_ZIP);
                log("Downloaded " + bytes.length + " bytes. Importing...");
                GreekImportStats stats = importMorphGntZipBytes(bytes);
                log(stats.summary());
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("Download/import Greek failed", ex);
            }
        }).start();
    }

    private byte[] download(String url) throws IOException {
        URLConnection c = new URL(url).openConnection();
        c.setRequestProperty("User-Agent", "BibleReaderApp/3.0");
        c.setConnectTimeout(20000);
        c.setReadTimeout(60000);
        try (InputStream in = c.getInputStream(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
            return out.toByteArray();
        }
    }

    private void importUsfmZipOrFolder() {
        JFileChooser ch = new JFileChooser(new File("."));
        ch.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        File f = ch.getSelectedFile();
        new Thread(() -> {
            try {
                int count = f.isDirectory() ? importUsfmFolder(f) : importUsfmZipBytes(Files.readAllBytes(f.toPath()));
                log("Imported " + count + " verses from USFM.");
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("USFM import failed", ex);
            }
        }).start();
    }

    private int importUsfmFolder(File dir) throws IOException {
        int count = 0;
        File[] files = dir.listFiles();
        if (files == null) return 0;
        for (File f : files) {
            if (f.isDirectory()) count += importUsfmFolder(f);
            else if (isTextLike(f.getName())) count += parseUsfm(new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8));
        }
        saveData();
        return count;
    }

    private boolean isTextLike(String name) {
        String n = name.toLowerCase(Locale.ROOT);
        return n.endsWith(".usfm") || n.endsWith(".sfm") || n.endsWith(".txt");
    }

    private int importUsfmZipBytes(byte[] bytes) throws IOException {
        int count = 0;
        try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null) {
                if (!e.isDirectory() && isTextLike(e.getName())) count += parseUsfm(new String(readAll(zin), StandardCharsets.UTF_8));
            }
        }
        saveData();
        return count;
    }

    private byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
        return out.toByteArray();
    }

    private int parseUsfm(String txt) {
        int imported = 0;
        String book = "";
        int chapter = 0;
        int pendingVerse = -1;
        StringBuilder pending = new StringBuilder();
        txt = txt.replace("\uFEFF", "");

        for (String raw : txt.split("\\R")) {
            String line = raw.trim();
            if (line.isEmpty()) continue;

            if (hasUsfmMarker(line, "id")) {
                String rest = removeLeadingUsfmMarker(line, "id");
                String[] parts = rest.split("\\s+", 2);
                if (parts.length > 0 && book.isEmpty()) book = bookCodeToName(parts[0].replaceAll("[^A-Za-z0-9]", ""));
                continue;
            }

            if (hasUsfmMarker(line, "toc1")) { book = normalizeBookName(removeLeadingUsfmMarker(line, "toc1")); continue; }
            if (hasUsfmMarker(line, "h") && book.isEmpty()) { book = normalizeBookName(removeLeadingUsfmMarker(line, "h")); continue; }

            if (hasUsfmMarker(line, "c")) {
                if (pendingVerse > 0 && !book.isEmpty() && chapter > 0) {
                    String cleaned = cleanUsfmText(pending.toString());
                    if (!cleaned.isEmpty()) { data.putVerse(new Verse(book, chapter, pendingVerse, cleaned)); imported++; }
                }
                pending.setLength(0);
                pendingVerse = -1;
                String rest = removeLeadingUsfmMarker(line, "c");
                String[] parts = rest.split("\\s+", 2);
                try { chapter = Integer.parseInt(parts[0].replaceAll("[^0-9]", "")); } catch (Exception ignored) {}
                if (parts.length > 1) line = parts[1].trim(); else continue;
            }

            List<VerseMarker> markers = findVerseMarkers(line);
            if (!markers.isEmpty()) {
                for (int i = 0; i < markers.size(); i++) {
                    VerseMarker current = markers.get(i);
                    if (pendingVerse > 0) {
                        String cleaned = cleanUsfmText(pending.toString());
                        if (!book.isEmpty() && chapter > 0 && !cleaned.isEmpty()) { data.putVerse(new Verse(book, chapter, pendingVerse, cleaned)); imported++; }
                        pending.setLength(0);
                    }
                    pendingVerse = current.verse;
                    int textStart = current.end;
                    int textEnd = (i + 1 < markers.size()) ? markers.get(i + 1).start : line.length();
                    if (textStart >= 0 && textStart <= textEnd && textEnd <= line.length()) pending.append(line, textStart, textEnd).append(' ');
                }
            } else if (pendingVerse > 0) {
                pending.append(line).append(' ');
            }
        }

        if (pendingVerse > 0 && !book.isEmpty() && chapter > 0) {
            String cleaned = cleanUsfmText(pending.toString());
            if (!cleaned.isEmpty()) { data.putVerse(new Verse(book, chapter, pendingVerse, cleaned)); imported++; }
        }

        return imported;
    }

    private boolean hasUsfmMarker(String line, String marker) {
        String trimmed = line == null ? "" : line.trim();
        String prefix = "\\" + marker;
        if (!trimmed.startsWith(prefix)) return false;
        return trimmed.length() == prefix.length() || Character.isWhitespace(trimmed.charAt(prefix.length()));
    }

    private String removeLeadingUsfmMarker(String line, String marker) {
        String trimmed = line == null ? "" : line.trim();
        String prefix = "\\" + marker;
        return trimmed.startsWith(prefix) ? trimmed.substring(prefix.length()).trim() : trimmed;
    }

    private List<VerseMarker> findVerseMarkers(String line) {
        List<VerseMarker> markers = new ArrayList<>();
        int i = 0;
        while (i < line.length()) {
            int idx = line.indexOf("\\v", i);
            if (idx < 0) break;
            int cursor = idx + 2;
            while (cursor < line.length() && Character.isWhitespace(line.charAt(cursor))) cursor++;
            int numberStart = cursor;
            while (cursor < line.length() && Character.isDigit(line.charAt(cursor))) cursor++;
            if (numberStart == cursor) { i = idx + 2; continue; }
            String numText = line.substring(numberStart, cursor);
            while (cursor < line.length() && (line.charAt(cursor) == '-' || line.charAt(cursor) == '–' || Character.isDigit(line.charAt(cursor)))) cursor++;
            while (cursor < line.length() && Character.isWhitespace(line.charAt(cursor))) cursor++;
            try { markers.add(new VerseMarker(idx, cursor, Integer.parseInt(numText))); } catch (Exception ignored) {}
            i = cursor;
        }
        return markers;
    }

    private String cleanUsfmText(String s) {
        if (s == null) return "";
        String cleaned = removeUsfmSpan(removeUsfmSpan(s, "\\f", "\\f*"), "\\x", "\\x*");
        cleaned = cleanUsfmWordAttributes(cleaned);
        cleaned = cleaned.replaceAll("\\\\[a-zA-Z0-9]+\\*", " ");
        cleaned = cleaned.replaceAll("\\\\[a-zA-Z]+[0-9]*\\s*", " ");
        cleaned = cleaned.replaceAll("\\|[^\\s]+", " ");
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned;
    }

    private String removeUsfmSpan(String text, String startMarker, String endMarker) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            int start = text.indexOf(startMarker, i);
            if (start < 0) { out.append(text.substring(i)); break; }
            out.append(text, i, start);
            int end = text.indexOf(endMarker, start + startMarker.length());
            if (end < 0) break;
            i = end + endMarker.length();
        }
        return out.toString();
    }

    private String cleanUsfmWordAttributes(String text) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < text.length()) {
            int markerStart = text.indexOf("\\w ", i);
            if (markerStart < 0) { out.append(text.substring(i)); break; }
            out.append(text, i, markerStart);
            int contentStart = markerStart + 3;
            int markerEnd = text.indexOf("\\w*", contentStart);
            if (markerEnd < 0) { out.append(text.substring(markerStart)); break; }
            String inside = text.substring(contentStart, markerEnd).trim();
            int pipe = inside.indexOf('|');
            if (pipe >= 0) inside = inside.substring(0, pipe).trim();
            out.append(inside).append(' ');
            i = markerEnd + 3;
        }
        return out.toString();
    }

    private String bookCodeToName(String code) {
        Map<String, String> m = bookCodeMap();
        return m.getOrDefault(code.toUpperCase(Locale.ROOT), normalizeBookName(code));
    }

    private Map<String, String> bookCodeMap() {
        String[][] pairs = {
                {"GEN", "Genesis"}, {"EXO", "Exodus"}, {"LEV", "Leviticus"}, {"NUM", "Numbers"}, {"DEU", "Deuteronomy"},
                {"JOS", "Joshua"}, {"JDG", "Judges"}, {"RUT", "Ruth"}, {"1SA", "1 Samuel"}, {"2SA", "2 Samuel"},
                {"1KI", "1 Kings"}, {"2KI", "2 Kings"}, {"1CH", "1 Chronicles"}, {"2CH", "2 Chronicles"}, {"EZR", "Ezra"},
                {"NEH", "Nehemiah"}, {"EST", "Esther"}, {"JOB", "Job"}, {"PSA", "Psalms"}, {"PRO", "Proverbs"},
                {"ECC", "Ecclesiastes"}, {"SNG", "Song of Solomon"}, {"ISA", "Isaiah"}, {"JER", "Jeremiah"}, {"LAM", "Lamentations"},
                {"EZK", "Ezekiel"}, {"DAN", "Daniel"}, {"HOS", "Hosea"}, {"JOL", "Joel"}, {"AMO", "Amos"}, {"OBA", "Obadiah"},
                {"JON", "Jonah"}, {"MIC", "Micah"}, {"NAM", "Nahum"}, {"HAB", "Habakkuk"}, {"ZEP", "Zephaniah"}, {"HAG", "Haggai"},
                {"ZEC", "Zechariah"}, {"MAL", "Malachi"}, {"MAT", "Matthew"}, {"MRK", "Mark"}, {"LUK", "Luke"}, {"JHN", "John"},
                {"ACT", "Acts"}, {"ROM", "Romans"}, {"1CO", "1 Corinthians"}, {"2CO", "2 Corinthians"}, {"GAL", "Galatians"},
                {"EPH", "Ephesians"}, {"PHP", "Philippians"}, {"COL", "Colossians"}, {"1TH", "1 Thessalonians"}, {"2TH", "2 Thessalonians"},
                {"1TI", "1 Timothy"}, {"2TI", "2 Timothy"}, {"TIT", "Titus"}, {"PHM", "Philemon"}, {"HEB", "Hebrews"},
                {"JAS", "James"}, {"1PE", "1 Peter"}, {"2PE", "2 Peter"}, {"1JN", "1 John"}, {"2JN", "2 John"},
                {"3JN", "3 John"}, {"JUD", "Jude"}, {"REV", "Revelation"}
        };
        Map<String, String> m = new HashMap<>();
        for (String[] p : pairs) m.put(p[0], p[1]);
        return m;
    }

    private void importBibleCsv() {
        JFileChooser ch = new JFileChooser(new File("."));
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        try {
            int count = 0;
            for (String line : Files.readAllLines(ch.getSelectedFile().toPath(), StandardCharsets.UTF_8)) {
                if (line.trim().isEmpty()) continue;
                List<String> c = parseCsv(line);
                if (c.size() < 4 || c.get(0).equalsIgnoreCase("Book")) continue;
                data.putVerse(new Verse(normalizeBookName(c.get(0)), Integer.parseInt(c.get(1).trim()), Integer.parseInt(c.get(2).trim()), c.get(3).trim()));
                count++;
            }
            saveData();
            refreshEverything();
            log("Imported " + count + " Bible CSV verses.");
        } catch (Exception ex) {
            showError("Bible CSV import failed", ex);
        }
    }

    private void importGreekCsv() {
        JFileChooser ch = new JFileChooser(new File("."));
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        try {
            int count = 0;
            for (String line : Files.readAllLines(ch.getSelectedFile().toPath(), StandardCharsets.UTF_8)) {
                if (line.trim().isEmpty()) continue;
                List<String> c = parseCsv(line);
                if (c.size() < 5 || c.get(0).equalsIgnoreCase("Book")) continue;
                String book = normalizeGreekBookCode(c.get(0));
                if (book.isEmpty()) book = normalizeBookName(c.get(0));
                GreekEntry ge = new GreekEntry(book, Integer.parseInt(c.get(1).trim()), Integer.parseInt(c.get(2).trim()), c.get(3), c.get(4));
                data.greek.put(ge.key(), ge);
                count++;
            }
            saveData();
            refreshEverything();
            log("Imported " + count + " Greek CSV entries. Unique Greek verse references now imported: " + data.greek.size() + ".");
        } catch (Exception ex) {
            showError("Greek CSV import failed", ex);
        }
    }

    private List<String> parseCsv(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean q = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (q && i + 1 < line.length() && line.charAt(i + 1) == '"') { sb.append('"'); i++; }
                else q = !q;
            } else if (c == ',' && !q) {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out;
    }

    private void importMorphGntZipOrFolder() {
        JFileChooser ch = new JFileChooser(new File("."));
        ch.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        File f = ch.getSelectedFile();
        new Thread(() -> {
            try {
                GreekImportStats stats = f.isDirectory() ? importMorphFolder(f) : importMorphGntZipBytes(Files.readAllBytes(f.toPath()));
                log(stats.summary());
                SwingUtilities.invokeLater(this::refreshEverything);
            } catch (Exception ex) {
                showError("Greek import failed", ex);
            }
        }).start();
    }

    private GreekImportStats importMorphFolder(File dir) throws IOException {
        Map<String, List<String[]>> grouped = new TreeMap<>();
        GreekImportStats stats = new GreekImportStats();
        collectMorphFiles(dir, grouped, stats);
        stats.uniqueVerses = saveGreekGroups(grouped);
        saveData();
        return stats;
    }

    private void collectMorphFiles(File dir, Map<String, List<String[]>> grouped, GreekImportStats stats) throws IOException {
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (f.isDirectory()) collectMorphFiles(f, grouped, stats);
            else if (f.getName().toLowerCase(Locale.ROOT).endsWith(".txt")) parseMorphText(new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8), grouped, stats);
        }
    }

    private GreekImportStats importMorphGntZipBytes(byte[] bytes) throws IOException {
        Map<String, List<String[]>> grouped = new TreeMap<>();
        GreekImportStats stats = new GreekImportStats();
        try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
            ZipEntry e;
            while ((e = zin.getNextEntry()) != null) {
                if (!e.isDirectory() && e.getName().toLowerCase(Locale.ROOT).endsWith(".txt")) {
                    parseMorphText(new String(readAll(zin), StandardCharsets.UTF_8), grouped, stats);
                }
            }
        }
        stats.uniqueVerses = saveGreekGroups(grouped);
        saveData();
        return stats;
    }

    private void parseMorphText(String txt, Map<String, List<String[]>> grouped, GreekImportStats stats) {
        for (String raw : txt.split("\\R")) {
            String line = raw.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] cols = line.split("\\s+");
            if (cols.length < 2) { stats.skipped++; continue; }
            RefParts rp = greekRefFromMorphColumns(cols, stats);
            if (rp == null) { stats.skipped++; continue; }
            String key = rp.key();
            if (stats.firstReference.isEmpty()) stats.firstReference = key;
            stats.lastReference = key;
            stats.wordEntries++;
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(cols);
        }
    }

    private RefParts greekRefFromMorphColumns(String[] cols, GreekImportStats stats) {
        if (cols == null || cols.length == 0) return null;
        RefParts fromLoc = greekRefFromToken(cols[0], stats);
        if (fromLoc != null) return fromLoc;
        if (cols.length >= 3) {
            String book = normalizeGreekBookCode(cols[0]);
            if (!book.isEmpty()) {
                try {
                    return new RefParts(book, Integer.parseInt(cols[1].replaceAll("[^0-9]", "")), Integer.parseInt(cols[2].replaceAll("[^0-9]", "")));
                } catch (Exception ignored) {
                    return null;
                }
            }
        }
        return null;
    }

    private RefParts greekRefFromToken(String token, GreekImportStats stats) {
        if (token == null) return null;
        String raw = token.trim();
        String digits = raw.replaceAll("[^0-9]", "");
        if (digits.length() >= 6 && raw.matches(".*\\d.*")) {
            try {
                String book = ntBookByNumber(digits.substring(0, 2));
                if (book != null) {
                    return new RefParts(book, Integer.parseInt(digits.substring(2, 4)), Integer.parseInt(digits.substring(4, 6)));
                }
            } catch (Exception ignored) {}
        }

        Matcher m = Pattern.compile("^([1-3]?[A-Za-z]+)[^0-9A-Za-z]+(\\d+)[^0-9A-Za-z]+(\\d+).*$").matcher(raw);
        if (m.matches()) {
            String book = normalizeGreekBookCode(m.group(1));
            if (!book.isEmpty()) return new RefParts(book, Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)));
            if (stats != null) stats.unknownBookCodes.add(m.group(1));
        }
        return null;
    }

    private String normalizeGreekBookCode(String code) {
        if (code == null) return "";
        String k = code.trim().replaceAll("[^A-Za-z0-9]", "").toUpperCase(Locale.ROOT);
        if (k.isEmpty()) return "";
        Map<String, String> m = greekBookCodeMap();
        String book = m.get(k);
        if (book != null) return book;
        String normalized = normalizeBookName(code);
        for (String canonical : canonicalBooks()) if (canonical.equals(normalized)) return normalized;
        return "";
    }

    private Map<String, String> greekBookCodeMap() {
        Map<String, String> m = new HashMap<>();
        addGreekBookCodes(m, "Matthew", "MATT", "MT", "MAT");
        addGreekBookCodes(m, "Mark", "MARK", "MRK", "MAR", "MK");
        addGreekBookCodes(m, "Luke", "LUKE", "LUK");
        addGreekBookCodes(m, "John", "JOHN", "JHN", "JOH", "JN");
        addGreekBookCodes(m, "Acts", "ACTS", "ACT");
        addGreekBookCodes(m, "Romans", "ROM", "ROMANS");
        addGreekBookCodes(m, "1 Corinthians", "1COR", "1CO", "ICOR");
        addGreekBookCodes(m, "2 Corinthians", "2COR", "2CO", "IICOR");
        addGreekBookCodes(m, "Galatians", "GAL");
        addGreekBookCodes(m, "Ephesians", "EPH");
        addGreekBookCodes(m, "Philippians", "PHIL", "PHP", "PHI");
        addGreekBookCodes(m, "Colossians", "COL");
        addGreekBookCodes(m, "1 Thessalonians", "1TH", "1THESS", "1THESSALONIANS");
        addGreekBookCodes(m, "2 Thessalonians", "2TH", "2THESS", "2THESSALONIANS");
        addGreekBookCodes(m, "1 Timothy", "1TIM", "1TI");
        addGreekBookCodes(m, "2 Timothy", "2TIM", "2TI");
        addGreekBookCodes(m, "Titus", "TITUS", "TIT");
        addGreekBookCodes(m, "Philemon", "PHLM", "PHILEMON", "PHM");
        addGreekBookCodes(m, "Hebrews", "HEB");
        addGreekBookCodes(m, "James", "JAS", "JAMES", "JAM");
        addGreekBookCodes(m, "1 Peter", "1PET", "1PE");
        addGreekBookCodes(m, "2 Peter", "2PET", "2PE");
        addGreekBookCodes(m, "1 John", "1JOHN", "1JN");
        addGreekBookCodes(m, "2 John", "2JOHN", "2JN");
        addGreekBookCodes(m, "3 John", "3JOHN", "3JN");
        addGreekBookCodes(m, "Jude", "JUDE", "JUD");
        addGreekBookCodes(m, "Revelation", "REV", "REVELATION");
        return m;
    }

    private void addGreekBookCodes(Map<String, String> map, String book, String... codes) {
        for (String code : codes) map.put(code.toUpperCase(Locale.ROOT), book);
    }

    private int saveGreekGroups(Map<String, List<String[]>> grouped) {
        int count = 0;
        for (String key : grouped.keySet()) {
            List<String[]> rows = grouped.get(key);
            StringBuilder greek = new StringBuilder();
            StringBuilder details = new StringBuilder();
            for (String[] cols : rows) {
                String surface = cols.length > 5 ? cols[5] : cols[cols.length - 1];
                String lemma = cols.length > 6 ? cols[6] : "";
                greek.append(surface).append(' ');
                details.append(surface);
                if (!lemma.isEmpty()) details.append(" — lemma: ").append(lemma);
                if (cols.length > 2) details.append("; parse: ").append(cols[1]).append(" ").append(cols[2]);
                details.append("\n");
            }
            RefParts rp = parseRef(key);
            if (rp != null) {
                data.greek.put(key, new GreekEntry(rp.book, rp.chapter, rp.verse, greek.toString().trim(), details.toString().trim()));
                count++;
            }
        }
        return count;
    }

    private String ntBookByNumber(String n) {
        String[] books = {"Matthew", "Mark", "Luke", "John", "Acts", "Romans", "1 Corinthians", "2 Corinthians", "Galatians", "Ephesians", "Philippians", "Colossians", "1 Thessalonians", "2 Thessalonians", "1 Timothy", "2 Timothy", "Titus", "Philemon", "Hebrews", "James", "1 Peter", "2 Peter", "1 John", "2 John", "3 John", "Jude", "Revelation"};
        try {
            int idx = Integer.parseInt(n) - 1;
            return idx >= 0 && idx < books.length ? books[idx] : null;
        } catch (Exception e) {
            return null;
        }
    }


    private void browseProjectGutenbergBooksDialog() {
        JDialog dialog = new JDialog(this, "Project Gutenberg Book Browser", false);
        dialog.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        dialog.setSize(1100, 650);
        dialog.setLocationRelativeTo(this);

        JTextField searchField = new JTextField();
        searchField.setToolTipText("Search title, author, subject, bookshelf, or Book ID");
        JComboBox<String> languageFilter = new JComboBox<>(new String[]{"English", "All Languages"});
        JButton refreshCatalog = blackButton("Refresh Catalog");
        JButton importSelected = blackButton("Import Selected Book");
        JButton close = blackButton("Close");
        JLabel status = new JLabel("Downloading Project Gutenberg catalog...");

        DefaultTableModel model = new DefaultTableModel(new Object[]{"Book ID / Text#", "Title", "Author", "Language", "Subjects", "Bookshelves"}, 0) {
            public boolean isCellEditable(int row, int column) { return false; }
        };
        JTable table = new JTable(model);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.setAutoCreateRowSorter(true);
        table.setFillsViewportHeight(true);
        table.getColumnModel().getColumn(0).setPreferredWidth(95);
        table.getColumnModel().getColumn(1).setPreferredWidth(270);
        table.getColumnModel().getColumn(2).setPreferredWidth(180);
        table.getColumnModel().getColumn(3).setPreferredWidth(90);
        table.getColumnModel().getColumn(4).setPreferredWidth(280);
        table.getColumnModel().getColumn(5).setPreferredWidth(180);

        List<GutenbergBook>[] catalogHolder = new List[]{new ArrayList<>()};
        Runnable applyFilter = () -> {
            List<GutenbergBook> filtered = filterGutenbergCatalog(catalogHolder[0], searchField.getText(), String.valueOf(languageFilter.getSelectedItem()));
            model.setRowCount(0);
            int maxRows = 2000;
            for (int i = 0; i < filtered.size() && i < maxRows; i++) {
                GutenbergBook b = filtered.get(i);
                model.addRow(new Object[]{b.id, b.title, b.author, b.language, b.subjects, b.bookshelves});
            }
            String suffix = filtered.size() > maxRows ? " (showing first " + maxRows + ")" : "";
            status.setText("Loaded " + catalogHolder[0].size() + " catalog rows. Matching: " + filtered.size() + suffix + ".");
        };

        DocumentListener filterListener = new SimpleDocumentListener(applyFilter);
        searchField.getDocument().addDocumentListener(filterListener);
        languageFilter.addActionListener(e -> applyFilter.run());

        Runnable loadCatalog = () -> {
            refreshCatalog.setEnabled(false);
            importSelected.setEnabled(false);
            status.setText("Downloading Project Gutenberg catalog CSV...");
            model.setRowCount(0);
            SwingWorker<List<GutenbergBook>, Void> worker = new SwingWorker<List<GutenbergBook>, Void>() {
                protected List<GutenbergBook> doInBackground() throws Exception {
                    String csv = downloadGutenbergCatalogCsv();
                    return parseGutenbergCatalogCsv(csv);
                }
                protected void done() {
                    try {
                        catalogHolder[0] = get();
                        List<String> languages = new ArrayList<>();
                        languages.add("English");
                        languages.add("All Languages");
                        Set<String> seen = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                        for (GutenbergBook b : catalogHolder[0]) {
                            String lang = safe(b.language).trim();
                            if (!lang.isEmpty() && seen.add(lang) && !lang.equalsIgnoreCase("English")) languages.add(lang);
                        }
                        languageFilter.setModel(new DefaultComboBoxModel<>(languages.toArray(new String[0])));
                        languageFilter.setSelectedItem("English");
                        applyFilter.run();
                        log("Loaded Project Gutenberg catalog: " + catalogHolder[0].size() + " books.");
                    } catch (Exception ex) {
                        status.setText("Catalog download failed: " + ex.getMessage());
                        showError("Project Gutenberg catalog download failed", ex);
                    } finally {
                        refreshCatalog.setEnabled(true);
                        importSelected.setEnabled(true);
                    }
                }
            };
            worker.execute();
        };

        refreshCatalog.addActionListener(e -> loadCatalog.run());
        importSelected.addActionListener(e -> {
            int viewRow = table.getSelectedRow();
            if (viewRow < 0) {
                JOptionPane.showMessageDialog(dialog, "Select a Project Gutenberg book first.");
                return;
            }
            int row = table.convertRowIndexToModel(viewRow);
            String id = String.valueOf(model.getValueAt(row, 0));
            String title = String.valueOf(model.getValueAt(row, 1));
            String author = String.valueOf(model.getValueAt(row, 2));
            String displayTitle = title + (author.trim().isEmpty() ? "" : " — " + author.trim());
            LibraryDoc existing = data.findLibraryDoc(displayTitle);
            if (existing != null) {
                Object[] options = {"Open Existing", "Import Another Copy", "Cancel"};
                int choice = JOptionPane.showOptionDialog(dialog,
                        "This book is already imported:\n" + displayTitle + "\n\nOpen the existing copy, or import another copy?",
                        "Book Already Imported", JOptionPane.DEFAULT_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
                if (choice == 0) {
                    openImportedLibraryBook(displayTitle);
                    dialog.dispose();
                    return;
                }
                if (choice != 1) return;
            }

            refreshCatalog.setEnabled(false);
            importSelected.setEnabled(false);
            status.setText("Downloading plain text for Gutenberg book #" + id + "...");
            SwingWorker<String, Void> worker = new SwingWorker<String, Void>() {
                protected String doInBackground() throws Exception {
                    String text = downloadGutenbergTextById(id);
                    return importLibraryTextFromString(displayTitle, text);
                }
                protected void done() {
                    try {
                        String importedTitle = get();
                        refreshEverything();
                        openImportedLibraryBook(importedTitle);
                        status.setText("Imported: " + importedTitle);
                        log("Imported Project Gutenberg book: " + importedTitle + " (#" + id + ")");
                        JOptionPane.showMessageDialog(dialog, "Imported Project Gutenberg book:\n" + importedTitle);
                        dialog.dispose();
                    } catch (Exception ex) {
                        status.setText("Import failed: " + ex.getMessage());
                        showError("Project Gutenberg import failed", ex);
                    } finally {
                        refreshCatalog.setEnabled(true);
                        importSelected.setEnabled(true);
                    }
                }
            };
            worker.execute();
        });
        close.addActionListener(e -> dialog.dispose());

        JPanel top = new JPanel(new BorderLayout(8, 8));
        top.setBorder(new EmptyBorder(10, 10, 6, 10));
        JPanel filters = new JPanel(new BorderLayout(8, 8));
        filters.add(new JLabel("Search:"), BorderLayout.WEST);
        filters.add(searchField, BorderLayout.CENTER);
        JPanel filterActions = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        filterActions.add(new JLabel("Language:"));
        filterActions.add(languageFilter);
        filterActions.add(refreshCatalog);
        filters.add(filterActions, BorderLayout.EAST);
        top.add(filters, BorderLayout.CENTER);

        JPanel bottom = new JPanel(new BorderLayout(8, 8));
        bottom.setBorder(new EmptyBorder(6, 10, 10, 10));
        bottom.add(status, BorderLayout.CENTER);
        JPanel buttons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 0));
        buttons.add(importSelected);
        buttons.add(close);
        bottom.add(buttons, BorderLayout.EAST);

        dialog.add(top, BorderLayout.NORTH);
        dialog.add(new JScrollPane(table), BorderLayout.CENTER);
        dialog.add(bottom, BorderLayout.SOUTH);
        dialog.setVisible(true);
        loadCatalog.run();
    }

    private String downloadGutenbergCatalogCsv() throws IOException {
        return new String(download(GUTENBERG_CATALOG_CSV), StandardCharsets.UTF_8);
    }

    private List<GutenbergBook> parseGutenbergCatalogCsv(String csv) {
        List<List<String>> rows = parseCsvRows(csv);
        List<GutenbergBook> books = new ArrayList<>();
        if (rows.isEmpty()) return books;
        Map<String, Integer> headers = new HashMap<>();
        List<String> header = rows.get(0);
        for (int i = 0; i < header.size(); i++) headers.put(normalizeCatalogHeader(header.get(i)), i);
        for (int i = 1; i < rows.size(); i++) {
            List<String> row = rows.get(i);
            String id = firstCatalogValue(row, headers, "text", "textnumber", "bookid", "id");
            id = id.replaceAll("[^0-9]", "");
            String title = firstCatalogValue(row, headers, "title");
            if (id.isEmpty() || title.trim().isEmpty()) continue;
            String language = firstCatalogValue(row, headers, "language", "languages");
            books.add(new GutenbergBook(
                    id,
                    title.trim(),
                    firstCatalogValue(row, headers, "authors", "author", "creator").trim(),
                    language.trim(),
                    firstCatalogValue(row, headers, "subjects", "subject").trim(),
                    firstCatalogValue(row, headers, "bookshelves", "bookshelf").trim()
            ));
        }
        return books;
    }

    private List<List<String>> parseCsvRows(String csv) {
        List<List<String>> rows = new ArrayList<>();
        List<String> row = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < csv.length(); i++) {
            char ch = csv.charAt(i);
            if (quoted) {
                if (ch == '"') {
                    if (i + 1 < csv.length() && csv.charAt(i + 1) == '"') {
                        cell.append('"');
                        i++;
                    } else quoted = false;
                } else cell.append(ch);
            } else {
                if (ch == '"') quoted = true;
                else if (ch == ',') { row.add(cell.toString()); cell.setLength(0); }
                else if (ch == '\n' || ch == '\r') {
                    if (ch == '\r' && i + 1 < csv.length() && csv.charAt(i + 1) == '\n') i++;
                    row.add(cell.toString());
                    if (!row.isEmpty() && row.stream().anyMatch(v -> !v.trim().isEmpty())) rows.add(row);
                    row = new ArrayList<>();
                    cell.setLength(0);
                } else cell.append(ch);
            }
        }
        row.add(cell.toString());
        if (!row.isEmpty() && row.stream().anyMatch(v -> !v.trim().isEmpty())) rows.add(row);
        return rows;
    }

    private String normalizeCatalogHeader(String header) {
        return header == null ? "" : header.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    }

    private String firstCatalogValue(List<String> row, Map<String, Integer> headers, String... names) {
        for (String name : names) {
            Integer idx = headers.get(normalizeCatalogHeader(name));
            if (idx != null && idx >= 0 && idx < row.size()) return row.get(idx);
        }
        return "";
    }

    private List<GutenbergBook> filterGutenbergCatalog(List<GutenbergBook> catalog, String query, String language) {
        String q = safe(query).toLowerCase(Locale.ROOT).trim();
        boolean allLanguages = language == null || language.equals("All Languages");
        List<GutenbergBook> result = new ArrayList<>();
        for (GutenbergBook b : catalog) {
            if (!allLanguages && !matchesGutenbergLanguage(b.language, language)) continue;
            String haystack = (b.id + " " + b.title + " " + b.author + " " + b.subjects + " " + b.bookshelves).toLowerCase(Locale.ROOT);
            if (q.isEmpty() || haystack.contains(q)) result.add(b);
        }
        return result;
    }

    private boolean matchesGutenbergLanguage(String actual, String selected) {
        String a = safe(actual).trim();
        String s = safe(selected).trim();
        if (s.equalsIgnoreCase("English")) return a.equalsIgnoreCase("English") || a.equalsIgnoreCase("en") || a.toLowerCase(Locale.ROOT).contains("english");
        return a.equalsIgnoreCase(s);
    }

    private String downloadGutenbergTextById(String id) throws IOException {
        String cleanId = safe(id).replaceAll("[^0-9]", "");
        if (cleanId.isEmpty()) throw new IOException("Missing Project Gutenberg book ID.");
        String[] urls = {
                "https://www.gutenberg.org/files/" + cleanId + "/" + cleanId + "-0.txt",
                "https://www.gutenberg.org/files/" + cleanId + "/" + cleanId + ".txt",
                "https://www.gutenberg.org/ebooks/" + cleanId + ".txt.utf-8"
        };
        IOException last = null;
        for (String url : urls) {
            try {
                URLConnection connection = new URL(url).openConnection();
                connection.setRequestProperty("User-Agent", "BibleReaderApp/3.0");
                connection.setConnectTimeout(20000);
                connection.setReadTimeout(60000);
                if (connection instanceof HttpURLConnection) {
                    int code = ((HttpURLConnection) connection).getResponseCode();
                    if (code < 200 || code >= 300) throw new IOException("HTTP " + code + " from " + url);
                }
                byte[] bytes;
                try (InputStream in = connection.getInputStream()) { bytes = readAll(in); }
                if (bytes.length == 0) throw new IOException("Empty text file from " + url);
                return new String(bytes, StandardCharsets.UTF_8);
            } catch (IOException ex) {
                last = ex;
            }
        }
        throw new IOException("Could not download a Project Gutenberg plain text file for book #" + cleanId + ".", last);
    }

    private String importLibraryTextFromString(String title, String text) {
        String cleanTitle = safe(title).trim();
        if (cleanTitle.isEmpty()) cleanTitle = "Project Gutenberg Book";
        String uniqueTitle = cleanTitle;
        int copy = 2;
        while (data.findLibraryDoc(uniqueTitle) != null) uniqueTitle = cleanTitle + " (Copy " + (copy++) + ")";
        data.libraryDocs.add(new LibraryDoc(uniqueTitle, "Philosophy / Other", text == null ? "" : text));
        saveData();
        return uniqueTitle;
    }

    private void openImportedLibraryBook(String titleOrKey) {
        String title = safe(titleOrKey);
        if (title.startsWith("LIBRARY:")) title = title.substring("LIBRARY:".length());
        showLibraryDoc(title);
        selectLibraryTreeTitle(title);
        showCard("study");
    }

    private void selectLibraryTreeTitle(String title) {
        if (rootNode == null || libraryTree == null) return;
        Enumeration<?> nodes = rootNode.breadthFirstEnumeration();
        while (nodes.hasMoreElements()) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) nodes.nextElement();
            if (title.equals(libraryTitleFromTreePathPart(node.getUserObject()))) {
                TreePath path = new TreePath(node.getPath());
                libraryTree.setSelectionPath(path);
                libraryTree.scrollPathToVisible(path);
                return;
            }
        }
    }

    private void importLibraryText() {
        JFileChooser ch = new JFileChooser(new File("."));
        if (ch.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        File f = ch.getSelectedFile();
        String title = JOptionPane.showInputDialog(this, "Title:", stripExt(f.getName()));
        if (title == null || title.trim().isEmpty()) return;
        try {
            String body = new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8);
            data.libraryDocs.add(new LibraryDoc(title.trim(), "Philosophy / Other", body));
            saveData();
            refreshEverything();
            log("Imported library document: " + title);
        } catch (Exception ex) {
            showError("TXT import failed", ex);
        }
    }

    private String stripExt(String s) {
        int i = s.lastIndexOf('.');
        return i > 0 ? s.substring(0, i) : s;
    }

    private void createTemplates() {
        try {
            Files.write(Paths.get("bible_template.csv"), Collections.singletonList("Book,Chapter,Verse,Text"), StandardCharsets.UTF_8);
            Files.write(Paths.get("greek_template.csv"), Collections.singletonList("Book,Chapter,Verse,GreekText,Details"), StandardCharsets.UTF_8);
            log("Created bible_template.csv and greek_template.csv");
        } catch (Exception ex) {
            showError("Template creation failed", ex);
        }
    }

    private void clearBibleText() {
        if (JOptionPane.showConfirmDialog(this, "Clear Bible text only? Greek, notes, library docs, and profiles stay saved.", "Clear Bible", JOptionPane.YES_NO_OPTION) != JOptionPane.YES_OPTION) return;
        data.bible.clear();
        selectedBook = "";
        selectedChapter = 1;
        saveData();
        refreshEverything();
        log("Bible text cleared.");
    }

    private void exportNotes() {
        JFileChooser ch = new JFileChooser(new File("study_notes_export.txt"));
        if (ch.showSaveDialog(this) != JFileChooser.APPROVE_OPTION) return;
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FileOutputStream(ch.getSelectedFile()), StandardCharsets.UTF_8))) {
            pw.println("Bible Study Export");
            pw.println("Profile: " + currentProfile.name);
            pw.println("Exported: " + new Date());
            pw.println();
            pw.println("CATEGORIES");
            pw.println("========================================");
            for (String c : currentProfile.categories.keySet()) {
                pw.println(c + " " + colorHex(colorForCategory(c)));
                pw.println(currentProfile.categories.get(c));
                pw.println();
            }
            pw.println("HIGHLIGHTS / NOTES");
            pw.println("========================================");
            for (TextAnnotation a : currentProfile.annotations) {
                pw.println("Source: " + a.sourceTitle);
                pw.println("Type: " + a.type);
                if (!a.category.isEmpty()) pw.println("Category: " + a.category);
                if (!a.target.isEmpty()) pw.println("Target: " + a.target);
                pw.println("Selected: " + a.selectedText);
                pw.println("Note: " + a.note);
                pw.println("----------------------------------------");
            }
            pw.println("QUESTIONS");
            pw.println("========================================");
            for (StudyQuestion q : currentProfile.questions) {
                pw.println(q.sourceTitle + " - " + (q.answered ? "Answered" : "Unfinished"));
                pw.println("Selected: " + q.selectedText);
                pw.println(q.question);
                pw.println("----------------------------------------");
            }
            log("Exported notes to " + ch.getSelectedFile().getAbsolutePath());
        } catch (Exception ex) {
            showError("Export failed", ex);
        }
    }

    private void backupNow() {
        try {
            if (!BACKUP_DIR.exists()) BACKUP_DIR.mkdirs();
            saveData();
            File dest = new File(BACKUP_DIR, "bible_reader_backup_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".ser");
            Files.copy(DATA_FILE.toPath(), dest.toPath(), StandardCopyOption.REPLACE_EXISTING);
            log("Backup created: " + dest.getAbsolutePath());
        } catch (Exception ex) {
            showError("Backup failed", ex);
        }
    }

    private AppData loadData() {
        if (DATA_FILE.exists()) {
            try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(DATA_FILE))) {
                Object o = in.readObject();
                if (o instanceof AppData) return (AppData) o;
            } catch (Exception ignored) {}
        }
        return new AppData();
    }

    private void saveData() {
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(DATA_FILE))) {
            out.writeObject(data);
        } catch (Exception ex) {
            System.err.println("Save failed: " + ex.getMessage());
        }
    }

    private void repairLoadedData() {
        if (data == null) data = new AppData();
        if (data.bible == null) data.bible = new TreeMap<>();
        if (data.profiles == null) data.profiles = new TreeMap<>();
        if (data.libraryDocs == null) data.libraryDocs = new ArrayList<>();
        if (data.greek == null) data.greek = new TreeMap<>();
        if (data.modernViewEnabled == null) data.modernViewEnabled = Boolean.TRUE;
        for (Profile p : data.profiles.values()) if (p != null) repairProfile(p);
    }

    private void repairProfile(Profile p) {
        if (p == null) return;
        if (p.name == null) p.name = "";
        if (p.annotations == null) p.annotations = new ArrayList<>();
        if (p.questions == null) p.questions = new ArrayList<>();
        if (p.categories == null) p.categories = new TreeMap<>();
        if (p.categoryColors == null) p.categoryColors = new TreeMap<>();
        for (String c : p.categories.keySet()) p.categoryColors.putIfAbsent(c, categoryBlue.getRGB());
        if (p.visitCounts == null) p.visitCounts = new HashMap<>();
        if (p.pinnedItems == null) p.pinnedItems = new ArrayList<>();
        if (p.memoryVerses == null) p.memoryVerses = new ArrayList<>();
        if (p.bookmarks == null) p.bookmarks = new ArrayList<>();
        if (p.studyProjects == null) p.studyProjects = new TreeMap<>();
        if (p.chapterNotes == null) p.chapterNotes = new TreeMap<>();
        if (p.topicPages == null) p.topicPages = new ArrayList<>();
        if (p.recentlyOpened == null) p.recentlyOpened = new ArrayList<>();
        p.annotations.removeIf(Objects::isNull);
        p.questions.removeIf(Objects::isNull);
        p.bookmarks.removeIf(Objects::isNull);
        p.pinnedItems.removeIf(Objects::isNull);
        p.memoryVerses.removeIf(Objects::isNull);
        p.topicPages.removeIf(Objects::isNull);
        p.recentlyOpened.removeIf(Objects::isNull);
        for (RecentLocation loc : p.recentlyOpened) repairRecentLocation(loc);
        for (TopicPage topic : p.topicPages) repairTopicPage(topic);
        for (StudyBookmark b : p.bookmarks) repairBookmark(b);
        p.chapterNotes.values().removeIf(Objects::isNull);
        for (ChapterNote note : p.chapterNotes.values()) repairChapterNote(note);
        p.studyProjects.values().removeIf(Objects::isNull);
        for (StudyProject project : p.studyProjects.values()) repairStudyProject(project);
        for (PinnedItem item : p.pinnedItems) repairPinnedItem(item);
        for (MemoryVerse mv : p.memoryVerses) repairMemoryVerse(mv);
        for (StudyQuestion q : p.questions) repairQuestion(q);
        long fallbackBase = System.currentTimeMillis() - (long) p.annotations.size() * 1000L;
        for (int i = 0; i < p.annotations.size(); i++) {
            repairAnnotation(p.annotations.get(i), fallbackBase + (long) i * 1000L);
        }
        if (p.oldNotes != null && !p.oldNotes.isEmpty()) {
            for (StudyNote n : p.oldNotes) {
                if (n == null) continue;
                TextAnnotation a = new TextAnnotation(n.refKey, n.refKey, 0, 0, "", n.type, n.category, n.body, "");
                if (n.created != null) {
                    a.createdAt = n.created.getTime();
                    a.updatedAt = a.createdAt;
                    a.created = n.created;
                }
                repairAnnotation(a, System.currentTimeMillis());
                p.annotations.add(a);
            }
            p.oldNotes.clear();
        }
    }



    private void repairChapterNote(ChapterNote n) {
        if (n == null) return;
        if (n.id == null || n.id.trim().isEmpty()) n.id = UUID.randomUUID().toString();
        if (n.sourceKey == null) n.sourceKey = "";
        if (n.sourceTitle == null) n.sourceTitle = "";
        if (n.book == null) n.book = "";
        if (n.noteText == null) n.noteText = "";
        if (n.createdAt <= 0L) n.createdAt = System.currentTimeMillis();
        if (n.updatedAt <= 0L) n.updatedAt = n.createdAt;
        if (n.linkedCategoryNames == null) n.linkedCategoryNames = new ArrayList<>();
        if (n.linkedStudyProjectIds == null) n.linkedStudyProjectIds = new ArrayList<>();
        if (n.linkedStudyProjectNames == null) n.linkedStudyProjectNames = new ArrayList<>();
        if (n.linkedTopicPageIds == null) n.linkedTopicPageIds = new ArrayList<>();
        if (n.linkedTopicPageNames == null) n.linkedTopicPageNames = new ArrayList<>();
        if (n.highlights == null) n.highlights = new ArrayList<>();
        n.linkedCategoryNames.removeIf(Objects::isNull);
        n.linkedStudyProjectIds.removeIf(Objects::isNull);
        n.linkedStudyProjectNames.removeIf(Objects::isNull);
        n.linkedTopicPageIds.removeIf(Objects::isNull);
        n.linkedTopicPageNames.removeIf(Objects::isNull);
        n.highlights.removeIf(Objects::isNull);
        for (ChapterNoteHighlight h : n.highlights) repairChapterNoteHighlight(h, n.id);
    }

    private void repairChapterNoteHighlight(ChapterNoteHighlight h, String fallbackNoteId) {
        if (h == null) return;
        if (h.chapterNoteId == null || h.chapterNoteId.isEmpty()) h.chapterNoteId = fallbackNoteId;
        if (h.selectedText == null) h.selectedText = "";
        if (h.type == null) h.type = "";
        if (h.linkId == null) h.linkId = "";
        if (h.linkName == null) h.linkName = "";
        if (h.createdAt <= 0L) h.createdAt = System.currentTimeMillis();
    }

    private void repairTopicPage(TopicPage topic) {
        if (topic == null) return;
        if (topic.id == null || topic.id.trim().isEmpty()) topic.id = UUID.randomUUID().toString();
        if (topic.title == null || topic.title.trim().isEmpty()) topic.title = "Untitled Topic";
        if (topic.summary == null) topic.summary = "";
        if (topic.links == null) topic.links = new ArrayList<>();
        topic.links.removeIf(Objects::isNull);
        for (LinkedItem link : topic.links) repairLinkedItem(link);
        if (topic.createdAt <= 0L) topic.createdAt = System.currentTimeMillis();
    }

    private void repairLinkedItem(LinkedItem link) {
        if (link == null) return;
        if (link.type == null) link.type = "";
        if (link.ref == null) link.ref = "";
        if (link.label == null) link.label = "";
        if (link.createdAt <= 0L) link.createdAt = System.currentTimeMillis();
    }

    private void repairStudyProject(StudyProject p) {
        if (p == null) return;
        long now = System.currentTimeMillis();
        if (p.id == null || p.id.trim().isEmpty()) p.id = UUID.randomUUID().toString();
        if (p.title == null || p.title.trim().isEmpty()) p.title = "Untitled Study";
        if (p.description == null) p.description = "";
        if (p.annotationIds == null) p.annotationIds = new ArrayList<>();
        if (p.bookmarkIds == null) p.bookmarkIds = new ArrayList<>();
        if (p.memoryVerseIds == null) p.memoryVerseIds = new ArrayList<>();
        if (p.chapterNoteIds == null) p.chapterNoteIds = new ArrayList<>();
        if (p.projectNotes == null) p.projectNotes = new ArrayList<>();
        p.annotationIds.removeIf(Objects::isNull);
        p.bookmarkIds.removeIf(Objects::isNull);
        p.memoryVerseIds.removeIf(Objects::isNull);
        p.chapterNoteIds.removeIf(Objects::isNull);
        p.projectNotes.removeIf(Objects::isNull);
        for (ProjectNote note : p.projectNotes) repairProjectNote(note);
        if (p.createdAt <= 0L) p.createdAt = now;
        if (p.updatedAt <= 0L) p.updatedAt = p.createdAt;
    }

    private void repairProjectNote(ProjectNote n) {
        if (n == null) return;
        long now = System.currentTimeMillis();
        if (n.id == null || n.id.trim().isEmpty()) n.id = UUID.randomUUID().toString();
        if (n.title == null) n.title = "Project Note";
        if (n.body == null) n.body = "";
        if (n.sourceKey == null) n.sourceKey = "";
        if (n.sourceTitle == null) n.sourceTitle = "";
        if (n.selectedText == null) n.selectedText = "";
        if (n.end < n.start) n.end = n.start;
        if (n.createdAt <= 0L) n.createdAt = now;
        if (n.updatedAt <= 0L) n.updatedAt = n.createdAt;
    }

    private void repairBookmark(StudyBookmark b) {
        if (b == null) return;
        if (b.id == null || b.id.trim().isEmpty()) b.id = UUID.randomUUID().toString();
        if (b.title == null) b.title = "Bookmark";
        if (b.sourceKey == null) b.sourceKey = "";
        if (b.sourceTitle == null) b.sourceTitle = "";
        if (b.previewText == null) b.previewText = "";
        if (b.type == null || b.type.trim().isEmpty()) b.type = b.sourceKey.startsWith("BIBLE:") ? "Bible" : (b.sourceKey.startsWith("LIBRARY:") ? "Library" : "General");
        if (b.createdAt <= 0L) b.createdAt = System.currentTimeMillis();
        if (b.caretPosition < 0) b.caretPosition = 0;
        if (b.selectionEnd < b.selectionStart) {
            b.selectionStart = -1;
            b.selectionEnd = -1;
        }
    }

    private void repairMemoryVerse(MemoryVerse mv) {
        if (mv == null) return;
        if (mv.id == null || mv.id.trim().isEmpty()) mv.id = UUID.randomUUID().toString();
        if (mv.reference == null) mv.reference = "";
        if (mv.text == null) mv.text = "";
        if (mv.category == null) mv.category = "";
        if (mv.note == null) mv.note = "";
        if (mv.createdAt <= 0L) mv.createdAt = System.currentTimeMillis();
        if (mv.lastReviewedAt < 0L) mv.lastReviewedAt = 0L;
        if (mv.reviewCount < 0) mv.reviewCount = 0;
        if (mv.correctCount < 0) mv.correctCount = 0;
        if (mv.correctCount > mv.reviewCount) mv.correctCount = mv.reviewCount;
    }

    private void repairPinnedItem(PinnedItem item) {
        if (item == null) return;
        if (item.id == null || item.id.trim().isEmpty()) item.id = UUID.randomUUID().toString();
        if (item.sourceKey == null) item.sourceKey = "";
        if (item.sourceTitle == null) item.sourceTitle = "";
        if (item.selectedText == null) item.selectedText = "";
        if (item.note == null) item.note = "";
        if (item.annotationId == null) item.annotationId = "";
        if (item.createdAt <= 0L) item.createdAt = System.currentTimeMillis();
        if (item.end < item.start) item.end = item.start;
    }


    private void repairQuestion(StudyQuestion q) {
        if (q == null) return;
        if (q.annotationId == null) q.annotationId = "";
        if (q.sourceTitle == null) q.sourceTitle = "";
        if (q.selectedText == null) q.selectedText = "";
        if (q.question == null) q.question = "";
        q.questionType = normalizeQuestionType(q.questionType);
        if (q.answers == null) q.answers = new ArrayList<>();
        q.answers.removeIf(Objects::isNull);
        for (QuestionAnswer a : q.answers) repairQuestionAnswer(a);
        if (q.sourceKey == null) q.sourceKey = "";
        if (q.sourceKey.isEmpty()) {
            TextAnnotation a = annotationById(q.annotationId);
            if (a != null) q.sourceKey = safe(a.sourceKey);
        }
        if (q.sourceLocation == null) q.sourceLocation = q.sourceTitle;
        if (q.book == null) q.book = "";
        if (q.chapter < 0) q.chapter = 0;
        if (q.created == null) q.created = new Date();
    }

    private void repairQuestionAnswer(QuestionAnswer a) {
        if (a == null) return;
        if (a.id == null || a.id.trim().isEmpty()) a.id = UUID.randomUUID().toString();
        if (a.text == null) a.text = "";
        if (a.createdAt <= 0L) a.createdAt = System.currentTimeMillis();
        if (a.updatedAt < a.createdAt) a.updatedAt = a.createdAt;
    }

    private void repairAnnotation(TextAnnotation a, long fallbackMillis) {
        if (a == null) return;
        if (a.id == null || a.id.trim().isEmpty()) a.id = UUID.randomUUID().toString();
        if (a.sourceKey == null) a.sourceKey = "";
        if (a.sourceTitle == null) a.sourceTitle = "";
        if (a.selectedText == null) a.selectedText = "";
        if (a.type == null || a.type.trim().isEmpty()) a.type = "Note";
        if (a.category == null) a.category = "";
        if (a.note == null) a.note = "";
        if (a.target == null) a.target = "";
        if (a.book == null) a.book = "";
        if (a.chapter < 0) a.chapter = 0;
        if (a.wholeChapter && a.book.isEmpty() && a.sourceKey.startsWith("BIBLE:")) {
            RefParts rp = parseRef(a.sourceKey.substring("BIBLE:".length()) + ":1");
            if (rp != null) { a.book = rp.book; a.chapter = rp.chapter; }
        }
        if (a.links == null) a.links = new ArrayList<>();
        a.links.removeIf(Objects::isNull);
        for (LinkedItem link : a.links) repairLinkedItem(link);

        long fallback = fallbackMillis > 0L ? fallbackMillis : System.currentTimeMillis();
        if (a.createdAt <= 0L) {
            a.createdAt = a.created != null ? a.created.getTime() : fallback;
        }
        if (a.updatedAt <= 0L) a.updatedAt = a.createdAt;
        if (a.created == null) a.created = new Date(a.createdAt);
    }

    private void showError(String title, Exception ex) {
        ex.printStackTrace();
        SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this, title + ":\n" + ex.getMessage()));
        log(title + ": " + ex.getMessage());
    }


    private class MemoryVerseCellRenderer extends DefaultListCellRenderer {
        public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
            JLabel label = (JLabel) super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            MemoryVerse mv = value instanceof MemoryVerse ? (MemoryVerse) value : null;
            if (mv == null) {
                label.setText("");
                return label;
            }
            String category = safe(mv.category).isEmpty() ? "Uncategorized" : mv.category;
            String stats = mv.reviewCount + " review" + (mv.reviewCount == 1 ? "" : "s")
                    + " • " + mv.correctCount + " correct"
                    + (mv.lastReviewedAt > 0L ? " • Last reviewed: " + displayDate(mv.lastReviewedAt) : " • Not reviewed yet");
            label.setText("<html><b>" + esc(mv.reference) + "</b> • " + esc(category)
                    + "<br>" + esc(shorten(mv.text, 180))
                    + (safe(mv.note).isEmpty() ? "" : "<br><span style='color:#5f4035;'>Note: " + esc(shorten(mv.note, 130)) + "</span>")
                    + "<br><span style='color:#6d5b50;'>" + esc(stats) + "</span></html>");
            label.setBorder(new EmptyBorder(8, 10, 8, 10));
            return label;
        }
    }

    private class ChapterNoteCellRenderer extends DefaultListCellRenderer {
        public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean selected, boolean focus) {
            JPanel panel = new JPanel(new BorderLayout(8, 4));
            panel.setBorder(new EmptyBorder(8, 10, 8, 10));
            panel.setBackground(selected ? modernSelection : modernSurface);
            if (value instanceof ChapterNote) {
                ChapterNote n = (ChapterNote) value;
                JLabel top = new JLabel("<html><b>" + esc(chapterNoteReference(n)) + "</b> <span style='color:#6b5c54'>" + esc(safe(n.sourceTitle)) + "</span></html>");
                top.setFont(new Font("Segoe UI", Font.PLAIN, 13));
                JLabel preview = new JLabel("<html>" + esc(shortenLines(n.noteText, 160)) + "</html>");
                preview.setFont(new Font("Segoe UI", Font.PLAIN, 12));
                preview.setForeground(modernText);
                JLabel meta = new JLabel("Updated " + displayDate(n.updatedAt) + chapterNoteBadgeText(n));
                meta.setFont(new Font("Segoe UI", Font.PLAIN, 11));
                meta.setForeground(modernMutedText);
                panel.add(top, BorderLayout.NORTH);
                panel.add(preview, BorderLayout.CENTER);
                panel.add(meta, BorderLayout.SOUTH);
            }
            return panel;
        }
    }

    private String chapterNoteBadgeText(ChapterNote n) {
        java.util.List<String> badges = new ArrayList<>();
        if (n != null) {
            if (!n.linkedCategoryNames.isEmpty()) badges.add("Categories: " + String.join(", ", n.linkedCategoryNames));
            if (!n.linkedStudyProjectNames.isEmpty()) badges.add("Studies: " + String.join(", ", n.linkedStudyProjectNames));
            if (!n.linkedTopicPageNames.isEmpty()) badges.add("Topics: " + String.join(", ", n.linkedTopicPageNames));
        }
        return badges.isEmpty() ? "" : " • " + String.join(" • ", badges);
    }

    private class RecentAnnotationCellRenderer extends DefaultListCellRenderer {
        public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
            JLabel label = (JLabel) super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            RecentAnnotationListItem item = value instanceof RecentAnnotationListItem ? (RecentAnnotationListItem) value : null;
            TextAnnotation a = item == null ? null : item.annotation;
            if (a == null) {
                label.setText("");
                return label;
            }

            String category = a.category == null || a.category.trim().isEmpty() ? "" : " • Category: " + esc(a.category);
            String target = a.target == null || a.target.trim().isEmpty() ? "" : " • Target: " + esc(shorten(a.target, 70));
            String note = a.note == null || a.note.trim().isEmpty() ? "No note text" : shorten(a.note, 150);
            label.setText("<html><b>" + esc(a.type) + "</b> • " + esc(sourceTitleFor(a)) + category + target
                    + "<br><span style='color:#5f4035;'>Selected:</span> " + esc(shorten(a.selectedText, 170))
                    + "<br><span style='color:#5f4035;'>Note:</span> " + esc(note)
                    + "<br><span style='font-size:10px;'>Created " + esc(displayDate(a.createdAt))
                    + " • Updated " + esc(displayDate(a.updatedAt)) + "</span></html>");
            label.setBorder(new CompoundBorder(new MatteBorder(0, 7, 1, 0, colorForAnnotation(a)), new EmptyBorder(7, 8, 7, 8)));
            return label;
        }
    }

    private static class RecentAnnotationListItem {
        TextAnnotation annotation;
        RecentAnnotationListItem(TextAnnotation annotation) { this.annotation = annotation; }
        public String toString() {
            if (annotation == null) return "";
            String source = annotation.sourceTitle == null || annotation.sourceTitle.isEmpty() ? annotation.sourceKey : annotation.sourceTitle;
            return annotation.type + " | " + source + " | " + annotation.selectedText;
        }
    }

    private static class SimpleDocumentListener implements DocumentListener {
        private final Runnable action;
        SimpleDocumentListener(Runnable action) { this.action = action; }
        public void insertUpdate(DocumentEvent e) { action.run(); }
        public void removeUpdate(DocumentEvent e) { action.run(); }
        public void changedUpdate(DocumentEvent e) { action.run(); }
    }

    private class CategoryCellRenderer extends DefaultListCellRenderer {
        public Component getListCellRendererComponent(JList<?> list, Object value, int index, boolean isSelected, boolean cellHasFocus) {
            JLabel label = (JLabel) super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
            String raw = value == null ? "" : value.toString();
            String cat = selectedCategoryNameFromListValue(raw);
            Color c = colorForCategory(cat);
            label.setText("■  " + raw);
            label.setForeground(isSelected ? list.getSelectionForeground() : c.darker());
            label.setBorder(new CompoundBorder(new MatteBorder(0, 7, 0, 0, c), new EmptyBorder(4, 8, 4, 4)));
            return label;
        }
    }

    private static class AnnotationBubbleMarker {
        TextAnnotation annotation;
        java.util.List<TextAnnotation> annotations = new ArrayList<>();
        String markerText;
        int sourceOffset;
        int startOffset;
        int endOffset;

        AnnotationBubbleMarker(TextAnnotation annotation, int sourceOffset, int startOffset, int endOffset) {
            this(annotation, Collections.singletonList(annotation), "", sourceOffset, startOffset, endOffset);
        }

        AnnotationBubbleMarker(TextAnnotation annotation, java.util.List<TextAnnotation> annotations, String markerText, int sourceOffset, int startOffset, int endOffset) {
            this.annotation = annotation;
            if (annotations != null) this.annotations = new ArrayList<>(annotations);
            this.markerText = markerText == null ? "" : markerText;
            this.sourceOffset = sourceOffset;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        String popupKey() {
            if (annotations == null || annotations.isEmpty()) return annotation == null ? "" : annotation.id;
            StringBuilder sb = new StringBuilder();
            for (TextAnnotation a : annotations) {
                if (a == null) continue;
                if (sb.length() > 0) sb.append('|');
                sb.append(a.id);
            }
            return sb.toString();
        }
    }

    private static class AnnotationUnderlinePainter extends LayeredHighlighter.LayerPainter {
        private final Color color;

        AnnotationUnderlinePainter(Color color) {
            this.color = color == null ? Color.GRAY : color;
        }

        public void paint(Graphics g, int offs0, int offs1, Shape bounds, JTextComponent c) {
            try {
                Rectangle start = c.modelToView(offs0);
                Rectangle end = c.modelToView(Math.max(offs0, offs1 - 1));
                if (start == null || end == null) return;
                Rectangle r = start.union(end);
                paintLine(g, r);
            } catch (BadLocationException ignored) {}
        }

        public Shape paintLayer(Graphics g, int offs0, int offs1, Shape bounds, JTextComponent c, View view) {
            try {
                Shape shape = view.modelToView(offs0, Position.Bias.Forward, offs1, Position.Bias.Backward, bounds);
                Rectangle r = shape == null ? bounds.getBounds() : shape.getBounds();
                paintLine(g, r);
                return r;
            } catch (BadLocationException ex) {
                Rectangle r = bounds.getBounds();
                paintLine(g, r);
                return r;
            }
        }

        private void paintLine(Graphics g, Rectangle r) {
            if (r == null) return;
            Color old = g.getColor();
            g.setColor(color.darker());
            int y = Math.max(r.y, r.y + r.height - 3);
            g.fillRect(r.x, y, Math.max(1, r.width), 2);
            g.setColor(old);
        }
    }

    private static class ParsedReference {
        String bookKey;
        int chapter;
        Integer verse;

        ParsedReference(String bookKey, int chapter, Integer verse) {
            this.bookKey = bookKey;
            this.chapter = chapter;
            this.verse = verse;
        }
    }

    private static class ChapterRef {
        String book;
        int chapter;
        ChapterRef(String b, int c) { book = b; chapter = c; }
    }

    private static class RefParts implements Serializable {
        private static final long serialVersionUID = 1L;
        String book;
        int chapter;
        int verse;
        RefParts(String b, int c, int v) { book = b; chapter = c; verse = v; }
        String key() { return book + " " + chapter + ":" + verse; }
    }

    private static class PassageRef implements Serializable {
        private static final long serialVersionUID = 1L;
        String book;
        int chapter;
        int startVerse;
        int endVerse;
        PassageRef(String b, int c, int s, int e) { book = b; chapter = c; startVerse = s; endVerse = e; }
        String key() { return book + " " + chapter + ":" + startVerse; }
        String display() { return book + " " + chapter + ":" + startVerse + (endVerse == startVerse ? "" : "-" + endVerse); }
    }

    private static class GreekImportStats {
        int wordEntries;
        int uniqueVerses;
        int skipped;
        String firstReference = "";
        String lastReference = "";
        Set<String> unknownBookCodes = new TreeSet<>();
        String summary() {
            return "Imported Greek word/line entries: " + wordEntries
                    + " | Unique Greek verse references imported: " + uniqueVerses
                    + " | Entries skipped: " + skipped
                    + " | First reference: " + (firstReference.isEmpty() ? "(none)" : firstReference)
                    + " | Last reference: " + (lastReference.isEmpty() ? "(none)" : lastReference)
                    + " | Unknown book codes: " + (unknownBookCodes.isEmpty() ? "(none)" : unknownBookCodes);
        }
    }

    private static class SearchResultParts {
        String type;
        String ref;
        String title;
        String extra;
        SearchResultParts(String type, String ref, String title, String extra) {
            this.type = type == null ? "" : type;
            this.ref = ref == null ? "" : ref;
            this.title = title == null ? "" : title;
            this.extra = extra == null ? "" : extra;
        }
    }

    private static class VerseMarker {
        int start;
        int end;
        int verse;
        VerseMarker(int s, int e, int v) { start = s; end = e; verse = v; }
    }



    private static class CommandPaletteItem {
        String label;
        Runnable action;
        CommandPaletteItem(String label, Runnable action) { this.label = label; this.action = action; }
        public String toString() { return label; }
    }

    private static class NavigationLocation {
        String sourceKey;
        String sourceTitle;
        String selectedBook;
        int selectedChapter;
        int caretPosition;
        int selectionStart;
        int selectionEnd;
        NavigationLocation(String sourceKey, String sourceTitle, String selectedBook, int selectedChapter, int caretPosition, int selectionStart, int selectionEnd) {
            this.sourceKey = sourceKey == null ? "" : sourceKey;
            this.sourceTitle = sourceTitle == null ? "" : sourceTitle;
            this.selectedBook = selectedBook == null ? "" : selectedBook;
            this.selectedChapter = selectedChapter;
            this.caretPosition = caretPosition;
            this.selectionStart = selectionStart;
            this.selectionEnd = selectionEnd;
        }
        boolean samePlace(NavigationLocation other) {
            return other != null && sourceKey.equals(other.sourceKey) && caretPosition == other.caretPosition && selectionStart == other.selectionStart && selectionEnd == other.selectionEnd;
        }
    }

    private static class RecentLocation implements Serializable {
        private static final long serialVersionUID = 30L;
        String sourceKey;
        String sourceTitle;
        String selectedBook;
        int selectedChapter;
        int caretPosition;
        int selectionStart;
        int selectionEnd;
        long openedAt = System.currentTimeMillis();
        RecentLocation(String sourceKey, String sourceTitle, String selectedBook, int selectedChapter, int caretPosition, int selectionStart, int selectionEnd) {
            this.sourceKey = sourceKey == null ? "" : sourceKey;
            this.sourceTitle = sourceTitle == null ? this.sourceKey : sourceTitle;
            this.selectedBook = selectedBook == null ? "" : selectedBook;
            this.selectedChapter = selectedChapter;
            this.caretPosition = caretPosition;
            this.selectionStart = selectionStart;
            this.selectionEnd = selectionEnd;
        }
        public String toString() { return sourceTitle == null || sourceTitle.isEmpty() ? sourceKey : sourceTitle; }
    }

    private static class AppData implements Serializable {
        private static final long serialVersionUID = 30L;
        Map<String, TreeMap<Integer, TreeMap<Integer, Verse>>> bible = new TreeMap<>();
        Map<String, GreekEntry> greek = new TreeMap<>();
        Map<String, Profile> profiles = new TreeMap<>();
        List<LibraryDoc> libraryDocs = new ArrayList<>();
        Boolean modernViewEnabled = Boolean.TRUE;

        void putVerse(Verse v) {
            bible.computeIfAbsent(v.book, k -> new TreeMap<>()).computeIfAbsent(v.chapter, k -> new TreeMap<>()).put(v.verse, v);
        }

        Set<Integer> getChapters(String book) {
            return bible.containsKey(book) ? bible.get(book).keySet() : new TreeSet<>();
        }

        Map<Integer, Verse> getVerses(String b, int c) {
            return bible.containsKey(b) && bible.get(b).containsKey(c) ? bible.get(b).get(c) : new TreeMap<>();
        }

        Verse findVerse(String key) {
            try {
                int colon = key.lastIndexOf(':');
                String left = key.substring(0, colon);
                int verse = Integer.parseInt(key.substring(colon + 1));
                int sp = left.lastIndexOf(' ');
                String book = left.substring(0, sp);
                int chapter = Integer.parseInt(left.substring(sp + 1));
                return getVerses(book, chapter).get(verse);
            } catch (Exception e) {
                return null;
            }
        }

        LibraryDoc findLibraryDoc(String title) {
            for (LibraryDoc d : libraryDocs) if (d.title.equals(title)) return d;
            return null;
        }

        int totalVerseCount() {
            int c = 0;
            for (String b : bible.keySet()) for (Integer ch : getChapters(b)) c += getVerses(b, ch).size();
            return c;
        }
    }

    private static class Profile implements Serializable {
        private static final long serialVersionUID = 30L;
        String name;
        List<TextAnnotation> annotations = new ArrayList<>();
        List<StudyQuestion> questions = new ArrayList<>();
        List<PinnedItem> pinnedItems = new ArrayList<>();
        List<MemoryVerse> memoryVerses = new ArrayList<>();
        List<StudyBookmark> bookmarks = new ArrayList<>();
        List<TopicPage> topicPages = new ArrayList<>();
        List<RecentLocation> recentlyOpened = new ArrayList<>();
        Map<String, ChapterNote> chapterNotes = new TreeMap<>();
        Map<String, StudyProject> studyProjects = new TreeMap<>();
        Map<String, String> categories = new TreeMap<>();
        Map<String, Integer> categoryColors = new TreeMap<>();
        Map<String, Integer> visitCounts = new HashMap<>();
        List<StudyNote> oldNotes = new ArrayList<>();
        Profile(String n) { name = n; }
    }

    private static class ChapterNote implements Serializable {
        private static final long serialVersionUID = 30L;
        String id = UUID.randomUUID().toString();
        String sourceKey = "";
        String sourceTitle = "";
        String book = "";
        int chapter;
        String noteText = "";
        long createdAt = System.currentTimeMillis();
        long updatedAt = createdAt;
        List<String> linkedCategoryNames = new ArrayList<>();
        List<String> linkedStudyProjectIds = new ArrayList<>();
        List<String> linkedStudyProjectNames = new ArrayList<>();
        List<String> linkedTopicPageIds = new ArrayList<>();
        List<String> linkedTopicPageNames = new ArrayList<>();
        List<ChapterNoteHighlight> highlights = new ArrayList<>();
        String annotationId = "";
        public String toString() { return (book == null || book.isEmpty() ? sourceTitle : book + " " + chapter); }
    }

    private static class ChapterNoteHighlight implements Serializable {
        private static final long serialVersionUID = 30L;
        String chapterNoteId = "";
        int startOffset;
        int endOffset;
        String selectedText = "";
        String type = "";
        String linkId = "";
        String linkName = "";
        long createdAt = System.currentTimeMillis();
    }

    private static class LinkedItem implements Serializable {
        private static final long serialVersionUID = 30L;
        String type;
        String ref;
        String label;
        long createdAt = System.currentTimeMillis();

        LinkedItem(String type, String ref, String label) {
            this.type = type == null ? "" : type;
            this.ref = ref == null ? "" : ref;
            this.label = label == null ? "" : label;
        }

        public String toString() {
            return type + ": " + ref + (label == null || label.isEmpty() ? "" : " — " + label);
        }
    }

    private static class TopicPage implements Serializable {
        private static final long serialVersionUID = 30L;
        String id = UUID.randomUUID().toString();
        String title;
        String summary = "";
        List<LinkedItem> links = new ArrayList<>();
        long createdAt = System.currentTimeMillis();

        TopicPage(String title) {
            this.title = title == null ? "Untitled Topic" : title.trim();
            if (this.title.isEmpty()) this.title = "Untitled Topic";
        }

        public String toString() {
            return title;
        }
    }

    private static class StudyProject implements Serializable {
        private static final long serialVersionUID = 30L;
        String id;
        String title;
        String description;
        List<String> annotationIds = new ArrayList<>();
        List<String> bookmarkIds = new ArrayList<>();
        List<String> memoryVerseIds = new ArrayList<>();
        List<String> chapterNoteIds = new ArrayList<>();
        List<ProjectNote> projectNotes = new ArrayList<>();
        long createdAt;
        long updatedAt;

        public String toString() { return title == null || title.trim().isEmpty() ? "Untitled Study" : title; }
    }

    private static class ProjectNote implements Serializable {
        private static final long serialVersionUID = 30L;
        String id;
        String title;
        String body;
        String sourceKey;
        String sourceTitle;
        String selectedText;
        int start;
        int end;
        long createdAt;
        long updatedAt;
    }

    private static class StudySearchResult {
        String type;
        String projectId;
        String itemId;
        String sourceKey;
        String sourceTitle;
        int start;
        int end;
        String title;
        String preview;

        public String toString() {
            String project = projectId == null || projectId.isEmpty() ? "" : " • " + projectId;
            String source = sourceTitle == null || sourceTitle.isEmpty() ? sourceKey : sourceTitle;
            return type + project + " | " + (source == null ? "" : source) + " | " + (preview == null ? "" : preview);
        }
    }


    private static class StudyBookmark implements Serializable {
    private static final long serialVersionUID = 30L;
    String id;
    String title;
    String sourceKey;
    String sourceTitle;
    int caretPosition;
    int selectionStart;
    int selectionEnd;
    String previewText;
    String type;
    long createdAt;

    // Added for imported TXT / philosophy books:
    // saves the exact vertical scroll position of the reader.
    int viewportY;
    boolean hasViewportY;

    // Helps later sorting/display if the same bookmark is moved repeatedly.
    long updatedAt;
}

    private static class MemoryVerse implements Serializable {
        private static final long serialVersionUID = 30L;
        String id;
        String reference;
        String text;
        String category;
        String note;
        int reviewCount;
        int correctCount;
        long createdAt;
        long lastReviewedAt;
    }

    private static class PinnedItem implements Serializable {
        private static final long serialVersionUID = 30L;
        String id;
        String sourceKey;
        String sourceTitle;
        int start;
        int end;
        String selectedText;
        String note;
        String annotationId;
        long createdAt;
    }

    private static class TextAnnotation implements Serializable {
        private static final long serialVersionUID = 30L;
        String id = UUID.randomUUID().toString();
        String sourceKey;
        String sourceTitle;
        int start;
        int end;
        String selectedText;
        String type;
        String category;
        String note;
        String target;
        String book;
        int chapter;
        boolean wholeChapter;
        List<LinkedItem> links = new ArrayList<>();
        long createdAt;
        long updatedAt;
        Date created = new Date();

        TextAnnotation(String sourceKey, String sourceTitle, int start, int end, String selectedText, String type, String category, String note, String target) {
            long now = System.currentTimeMillis();
            this.createdAt = now;
            this.updatedAt = now;
            this.sourceKey = sourceKey == null ? "" : sourceKey;
            this.sourceTitle = sourceTitle == null ? "" : sourceTitle;
            this.start = start;
            this.end = end;
            this.selectedText = selectedText == null ? "" : selectedText;
            this.type = type == null || type.isEmpty() ? "Note" : type;
            this.category = category == null ? "" : category;
            this.note = note == null ? "" : note;
            this.target = target == null ? "" : target;
        }
    }

    private static class GreekEntry implements Serializable {
        private static final long serialVersionUID = 30L;
        String book;
        int chapter;
        int verse;
        String greekText;
        String details;

        GreekEntry(String b, int c, int v, String g, String d) {
            book = b;
            chapter = c;
            verse = v;
            greekText = g == null ? "" : g;
            details = d == null ? "" : d;
        }

        String key() { return book + " " + chapter + ":" + verse; }
    }

    private static class Verse implements Serializable {
        private static final long serialVersionUID = 30L;
        String book;
        String text;
        int chapter;
        int verse;
        Verse(String b, int c, int v, String t) { book = b; chapter = c; verse = v; text = t; }
        String key() { return book + " " + chapter + ":" + verse; }
    }

    private static class StudyQuestion implements Serializable {
        private static final long serialVersionUID = 31L;
        String annotationId;
        String sourceTitle;
        String selectedText;
        String question;
        String questionType = "discussion";
        List<QuestionAnswer> answers = new ArrayList<>();
        boolean answered = false;
        String sourceKey = "";
        String sourceLocation = "";
        String book = "";
        int chapter;
        boolean wholeChapter;
        Date created = new Date();

        StudyQuestion(String annotationId, String sourceTitle, String selectedText, String question) {
            this(annotationId, sourceTitle, selectedText, question, "discussion", "", sourceTitle, "", 0, false);
        }

        StudyQuestion(String annotationId, String sourceTitle, String selectedText, String question, String questionType, String sourceKey, String sourceLocation, String book, int chapter, boolean wholeChapter) {
            this.annotationId = annotationId;
            this.sourceTitle = sourceTitle;
            this.selectedText = selectedText;
            this.question = question;
            this.questionType = questionType == null || questionType.toLowerCase(Locale.ROOT).startsWith("p") ? "personal" : "discussion";
            this.sourceKey = sourceKey == null ? "" : sourceKey;
            this.sourceLocation = sourceLocation == null ? sourceTitle : sourceLocation;
            this.book = book == null ? "" : book;
            this.chapter = chapter;
            this.wholeChapter = wholeChapter;
        }
    }

    private static class QuestionAnswer implements Serializable {
        private static final long serialVersionUID = 31L;
        String id;
        String text;
        long createdAt;
        long updatedAt;

        QuestionAnswer(String text) {
            long now = System.currentTimeMillis();
            this.id = UUID.randomUUID().toString();
            this.text = text == null ? "" : text;
            this.createdAt = now;
            this.updatedAt = now;
        }
    }

    private static class StudyNote implements Serializable {
        private static final long serialVersionUID = 5L;
        String refKey;
        String type;
        String category;
        String body;
        Date created = new Date();
        StudyNote(String r, String t, String c, String b) { refKey = r; type = t; category = c == null ? "" : c; body = b; }
    }

    private static class GutenbergBook {
        String id;
        String title;
        String author;
        String language;
        String subjects;
        String bookshelves;
        GutenbergBook(String id, String title, String author, String language, String subjects, String bookshelves) {
            this.id = id == null ? "" : id;
            this.title = title == null ? "" : title;
            this.author = author == null ? "" : author;
            this.language = language == null ? "" : language;
            this.subjects = subjects == null ? "" : subjects;
            this.bookshelves = bookshelves == null ? "" : bookshelves;
        }
    }

    private static class LibraryDoc implements Serializable {
        private static final long serialVersionUID = 30L;
        String title;
        String type;
        String body;
        LibraryDoc(String t, String ty, String b) { title = t; type = ty; body = b; }
    }
}
